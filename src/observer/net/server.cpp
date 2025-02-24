/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021
//
#include "net/server.h"  // 包含服务器定义的头文件

// 包含网络编程所需的系统头文件
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <poll.h>

#include <memory>  // 包含智能指针等内存管理工具

// 包含项目内部的一些头文件，用于配置、日志、会话管理等
#include "common/ini_setting.h"
#include "common/io/io.h"
#include "common/lang/mutex.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "session/session_stage.h"
#include "net/communicator.h"
#include "net/cli_communicator.h"
#include "session/session.h"
#include "net/thread_handler.h"
#include "net/sql_task_handler.h"

using namespace common;  // 使用common命名空间

// 服务器参数构造函数
ServerParam::ServerParam()
{
  listen_addr        = INADDR_ANY;  // 监听所有地址
  max_connection_num = MAX_CONNECTION_NUM_DEFAULT;  // 最大连接数
  port               = PORT_DEFAULT;  // 默认端口
}

// 网络服务器类构造函数
NetServer::NetServer(const ServerParam &input_server_param) : Server(input_server_param) {}

// 网络服务器类析构函数
NetServer::~NetServer()
{
  if (started_) {
    shutdown();
  }
}

// 设置文件描述符为非阻塞模式的函数
int NetServer::set_non_block(int fd)
{
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    LOG_INFO("Failed to get flags of fd :%d. ", fd);
    return -1;
  }

  flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  if (flags == -1) {
    LOG_INFO("Failed to set non-block flags of fd :%d. ", fd);
    return -1;
  }
  return 0;
}

// 接受客户端连接的函数
void NetServer::accept(int fd)
{
  struct sockaddr_in addr;  // 客户端地址结构体
  socklen_t addrlen = sizeof(addr);

  int ret = 0;

  int client_fd = ::accept(fd, (struct sockaddr *)&addr, &addrlen);
  if (client_fd < 0) {
    LOG_ERROR("Failed to accept client's connection, %s", strerror(errno));
    return;
  }

  char ip_addr[24];
  if (inet_ntop(AF_INET, &addr.sin_addr, ip_addr, sizeof(ip_addr)) == nullptr) {
    LOG_ERROR("Failed to get ip address of client, %s", strerror(errno));
    ::close(client_fd);
    return;
  }
  stringstream address;
  address << ip_addr << ":" << addr.sin_port;
  string addr_str = address.str();

  ret = set_non_block(client_fd);
  if (ret < 0) {
    LOG_ERROR("Failed to set socket of %s as non blocking, %s", addr_str.c_str(), strerror(errno));
    ::close(client_fd);
    return;
  }

  if (!server_param_.use_unix_socket) {
    // unix socket不支持设置NODELAY
    int yes = 1;
    ret = setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    if (ret < 0) {
      LOG_ERROR("Failed to set socket of %s option as : TCP_NODELAY %s\n", addr_str.c_str(), strerror(errno));
      ::close(client_fd);
      return;
    }
  }

  Communicator *communicator = communicator_factory_.create(server_param_.protocol);

  RC rc = communicator->init(client_fd, make_unique<Session>(Session::default_session()), addr_str);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init communicator. rc=%s", strrc(rc));
    delete communicator;
    return;
  }

  LOG_INFO("Accepted connection from %s\n", communicator->addr());

  rc = thread_handler_->new_connection(communicator);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to handle new connection. rc=%s", strrc(rc));
    delete communicator;
    return;
  }
}

// 启动服务器的函数
int NetServer::start()
{
  if (server_param_.use_std_io) {
    return -1;
  } else if (server_param_.use_unix_socket) {
    return start_unix_socket_server();
  } else {
    return start_tcp_server();
  }
}

// 启动TCP服务器的函数
int NetServer::start_tcp_server()
{
  int ret = 0;
  struct sockaddr_in sa;

  server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_socket_ < 0) {
    LOG_ERROR("socket(): can not create server socket: %s.", strerror(errno));
    return -1;
  }

  int yes = 1;
  ret = setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  if (ret < 0) {
    LOG_ERROR("Failed to set socket option of reuse address: %s.", strerror(errno));
    ::close(server_socket_);
    return -1;
  }

  ret = set_non_block(server_socket_);
  if (ret < 0) {
    LOG_ERROR("Failed to set socket option non-blocking:%s. ", strerror(errno));
    ::close(server_socket_);
    return -1;
  }

  memset(&sa, 0, sizeof(sa));
  sa.sin_family      = AF_INET;
  sa.sin_port        = htons(server_param_.port);
  sa.sin_addr.s_addr = htonl(server_param_.listen_addr);

  ret = ::bind(server_socket_, (struct sockaddr *)&sa, sizeof(sa));
  if (ret < 0) {
    LOG_ERROR("bind(): can not bind server socket, %s", strerror(errno));
    ::close(server_socket_);
    return -1;
  }

  ret = listen(server_socket_, server_param_.max_connection_num);
  if (ret < 0) {
    LOG_ERROR("listen(): can not listen server socket, %s", strerror(errno));
    ::close(server_socket_);
    return -1;
  }
  LOG_INFO("Listen on port %d", server_param_.port);

  started_ = true;
  LOG_INFO("Observer start success");
  return 0;
}
// 启动Unix域套接字服务器的函数
int NetServer::start_unix_socket_server()
{
  int ret = 0;  // 用于存储函数返回值
  server_socket_ = socket(PF_UNIX, SOCK_STREAM, 0);  // 创建Unix域套接字
  if (server_socket_ < 0) {
    LOG_ERROR("socket(): can not create unix socket: %s.", strerror(errno));  // 记录错误日志
    return -1;  // 返回错误码
  }

  ret = set_non_block(server_socket_);  // 设置非阻塞模式
  if (ret < 0) {
    LOG_ERROR("Failed to set socket option non-blocking:%s. ", strerror(errno));  // 记录错误日志
    ::close(server_socket_);  // 关闭套接字
    return -1;  // 返回错误码
  }

  unlink(server_param_.unix_socket_path.c_str());  // 如果存在旧的套接字文件，删除它

  struct sockaddr_un sockaddr;  // Unix域套接字地址结构体
  memset(&sockaddr, 0, sizeof(sockaddr));  // 清零
  sockaddr.sun_family = PF_UNIX;  // 设置地址族为Unix
  snprintf(sockaddr.sun_path, sizeof(sockaddr.sun_path), "%s", server_param_.unix_socket_path.c_str());  // 设置路径

  ret = ::bind(server_socket_, (struct sockaddr *)&sockaddr, sizeof(sockaddr));  // 绑定地址
  if (ret < 0) {
    LOG_ERROR("bind(): can not bind server socket(path=%s), %s", sockaddr.sun_path, strerror(errno));  // 记录错误日志
    ::close(server_socket_);  // 关闭套接字
    return -1;  // 返回错误码
  }

  ret = listen(server_socket_, server_param_.max_connection_num);  // 开始监听
  if (ret < 0) {
    LOG_ERROR("listen(): can not listen server socket, %s", strerror(errno));  // 记录错误日志
    ::close(server_socket_);  // 关闭套接字
    return -1;  // 返回错误码
  }
  LOG_INFO("Listen on unix socket: %s", sockaddr.sun_path);  // 记录日志

  started_ = true;  // 标记服务器已启动
  LOG_INFO("Observer start success");  // 记录启动成功的日志
  return 0;  // 返回成功码
}

// 服务函数，用于启动服务器并开始监听
int NetServer::serve()
{
  thread_handler_ = ThreadHandler::create(server_param_.thread_handling.c_str());  // 创建线程处理器
  if (thread_handler_ == nullptr) {
    LOG_ERROR("Failed to create thread handler: %s", server_param_.thread_handling.c_str());  // 记录错误日志
    return -1;  // 返回错误码
  }

  RC rc = thread_handler_->start();  // 启动线程处理器
  if (OB_FAIL(rc)) {
    LOG_ERROR("failed to start thread handler: %s", strrc(rc));  // 记录错误日志
    return -1;  // 返回错误码
  }

  int retval = start();  // 启动服务器
  if (retval == -1) {
    LOG_PANIC("Failed to start network");  // 记录严重错误日志
    exit(-1);  // 退出程序
  }

  if (!server_param_.use_std_io) {
    struct pollfd poll_fd;  // poll函数的文件描述符结构体
    poll_fd.fd = server_socket_;  // 服务器套接字
    poll_fd.events = POLLIN;  // 监听读事件
    poll_fd.revents = 0;  // 重置事件

    while (started_) {  // 只要服务器在运行
      int ret = poll(&poll_fd, 1, 500);  // 使用poll等待连接
      if (ret < 0) {
        LOG_WARN("[listen socket] poll error. fd = %d, ret = %d, error=%s", poll_fd.fd, ret, strerror(errno));  // 记录警告日志
        break;  // 退出循环
      } else if (0 == ret) {
        // LOG_TRACE("poll timeout. fd = %d", poll_fd.fd);  // poll超时，无操作
        continue;  // 继续循环
      }

      if (poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) {  // 如果有错误事件
        LOG_ERROR("poll error. fd = %d, revents = %d", poll_fd.fd, poll_fd.revents);  // 记录错误日志
        break;  // 退出循环
      }

      this->accept(server_socket_);  // 接受连接
    }
  }

  thread_handler_->stop();  // 停止线程处理器
  thread_handler_->await_stop();  // 等待线程处理器完全停止
  delete thread_handler_;  // 删除线程处理器
  thread_handler_ = nullptr;  // 清空指针

  started_ = false;  // 标记服务器已停止
  LOG_INFO("NetServer quit");  // 记录退出日志
  return 0;  // 返回成功码
}

// 关闭服务器的函数
void NetServer::shutdown()
{
  LOG_INFO("NetServer shutting down");  // 记录关闭日志

  // cleanup  // 清理资源
  started_ = false;  // 标记服务器未启动
}

//////////////////////////////////////////////////////////////////////////////////

// 命令行服务器类构造函数
CliServer::CliServer(const ServerParam &input_server_param) : Server(input_server_param) {}

// 命令行服务器类析构函数
CliServer::~CliServer()
{
  if (started_) {
    shutdown();
  }
}

// 命令行服务器的服务函数
int CliServer::serve()
{
  CliCommunicator communicator;  // 创建命令行通信器

  RC rc = communicator.init(STDIN_FILENO, make_unique<Session>(Session::default_session()), "stdin");  // 初始化通信器
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to init cli communicator. rc=%s", strrc(rc));  // 记录警告日志
    return -1;  // 返回错误码
  }

  started_ = true;  // 标记服务器已启动

  SqlTaskHandler task_handler;  // 创建SQL任务处理器
  while (started_ && !communicator.exit()) {  // 只要服务器在运行且通信器未请求退出
    rc = task_handler.handle_event(&communicator);  // 处理事件
    if (OB_FAIL(rc)) {
      started_ = false;  // 标记服务器未启动
    }
  }

  started_ = false;  // 标记服务器已停止
  return 0;  // 返回成功码
}

// 命令行服务器的关闭函数
void CliServer::shutdown()
{
  LOG_INFO("CliServer shutting down");  // 记录关闭日志

  // cleanup  // 清理资源
  started_ = false;  // 标记服务器未启动
}