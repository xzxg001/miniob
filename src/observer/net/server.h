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
// Created by Longda on 2021/4/1.
//

#pragma once

#include "net/server_param.h"

class Communicator;
class ThreadHandler;

/**
 * @brief 负责接收客户端消息并创建任务
 * @ingroup Communicator
 * @details 当前支持网络连接，有TCP和Unix Socket两种方式。通过命令行参数来指定使用哪种方式。
 * 启动后监听端口或unix socket，使用libevent来监听事件，当有新的连接到达时，创建一个Communicator对象进行处理。
 */
// Server基类，定义了服务器的基本功能和属性
class Server
{
public:
  // 构造函数，接收服务器参数
  Server(const ServerParam &input_server_param) : server_param_(input_server_param) {}
  virtual ~Server() {}

  // 纯虚函数，用于在派生类中实现具体的服务启动逻辑
  virtual int serve() = 0;
  // 纯虚函数，用于在派生类中实现服务器关闭逻辑
  virtual void shutdown() = 0;

protected:
  ServerParam server_param_;  ///< 服务器启动参数
};

// NetServer类，继承自Server，用于处理网络连接
class NetServer : public Server
{
public:
  // 构造函数
  NetServer(const ServerParam &input_server_param);
  // 析构函数
  virtual ~NetServer();

  // 覆盖基类的serve函数，启动网络服务
  int serve() override;
  // 覆盖基类的shutdown函数，关闭网络服务
  void shutdown() override;

private:
  /**
   * @brief 接收到新的连接时，调用此函数创建Communicator对象
   * @details 此函数作为libevent中监听套接字对应的回调函数
   * @param fd libevent回调函数传入的参数，即监听套接字
   * @param ev 本次触发的事件，通常是EV_READ
   */
  // 接收新的连接时调用此函数创建Communicator对象
  void accept(int fd);

  // 将socket描述符设置为非阻塞模式
  int set_non_block(int fd);

  // 启动服务，可能是TCP或Unix Socket
  int start();

  // 启动TCP服务
  int start_tcp_server();

  // 启动Unix Socket服务
  int start_unix_socket_server();

private:
  volatile bool started_ = false;  // 标记服务器是否已启动

  int server_socket_ = -1;  ///< 监听套接字的文件描述符

  CommunicatorFactory communicator_factory_;  ///< 创建新的Communicator对象的工厂
  ThreadHandler *thread_handler_ = nullptr;  ///< 线程处理器，用于处理事件
};

// CliServer类，继承自Server，用于处理命令行接口
class CliServer : public Server
{
public:
  // 构造函数
  CliServer(const ServerParam &input_server_param);
  // 析构函数
  virtual ~CliServer();

  // 覆盖基类的serve函数，启动命令行服务
  int serve() override;
  // 覆盖基类的shutdown函数，关闭命令行服务
  void shutdown() override;

private:
  volatile bool started_ = false;  // 标记服务器是否已启动
};