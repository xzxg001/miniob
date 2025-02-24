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
// Created by Wangyunlai on 2024/01/10.
//

// 引入必要的头文件
#include <poll.h> // 提供poll函数，用于I/O多路复用

#include "net/one_thread_per_connection_thread_handler.h" // 定义了OneThreadPerConnectionThreadHandler类
#include "common/log/log.h" // 定义了日志记录功能
#include "common/lang/thread.h" // 定义了线程类
#include "common/lang/mutex.h" // 定义了互斥锁
#include "common/lang/chrono.h" // 定义了时间相关功能
#include "common/thread/thread_util.h" // 定义了线程工具函数
#include "net/communicator.h" // 定义了Communicator类
#include "net/sql_task_handler.h" // 定义了SqlTaskHandler类

using namespace common; // 使用common命名空间

/**
 * @brief 工作线程类，负责处理每个客户端连接
 */
class Worker
{
public:
  // 构造函数
  Worker(ThreadHandler &host, Communicator *communicator) 
    : host_(host), communicator_(communicator)
  {}
  
  // 析构函数
  ~Worker()
  {
    if (thread_ != nullptr) {
      stop(); // 停止线程
      join(); // 等待线程结束
    }
  }

  // 开启线程
  RC start()
  {
    thread_ = new thread(std::ref(*this)); // 创建线程，绑定当前对象的operator()作为线程函数
    return RC::SUCCESS;
  }

  // 停止线程
  RC stop()
  {
    running_ = false; // 设置运行标志为false，通知线程停止
    return RC::SUCCESS;
  }

  // 等待线程结束
  RC join()
  {
    if (thread_) {
      if (thread_->get_id() == this_thread::get_id()) {
        thread_->detach(); // 如果当前线程join当前线程，就会卡死，所以detach
      } else {
        thread_->join(); // 等待线程结束
      }
      delete thread_; // 删除线程对象
      thread_ = nullptr;
    }
    return RC::SUCCESS;
  }

  // 线程函数
  void operator()()
  {
    LOG_INFO("worker thread start. communicator = %p", communicator_);
    int ret = thread_set_name("SQLWorker"); // 设置线程名称
    if (ret != 0) {
      LOG_WARN("failed to set thread name. ret = %d", ret);
    }

    struct pollfd poll_fd;
    poll_fd.fd = communicator_->fd(); // 获取文件描述符
    poll_fd.events = POLLIN; // 设置监听事件
    poll_fd.revents = 0;

    while (running_) {
      int ret = poll(&poll_fd, 1, 500); // 使用poll监听文件描述符
      if (ret < 0) {
        LOG_WARN("poll error. fd = %d, ret = %d, error=%s", poll_fd.fd, ret, strerror(errno));
        break;
      } else if (0 == ret) {
        // LOG_TRACE("poll timeout. fd = %d", poll_fd.fd);
        continue;
      }

      if (poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        LOG_WARN("poll error. fd = %d, revents = %d", poll_fd.fd, poll_fd.revents);
        break;
      }

      RC rc = task_handler_.handle_event(communicator_); // 处理事件
      if (OB_FAIL(rc)) {
        LOG_ERROR("handle error. rc = %s", strrc(rc));
        break;
      }
    }

    LOG_INFO("worker thread stop. communicator = %p", communicator_);
    host_.close_connection(communicator_); /// 连接关闭后，当前对象会被删除
  }

private:
  ThreadHandler &host_; // 线程处理主机
  SqlTaskHandler task_handler_; // SQL任务处理器
  Communicator *communicator_ = nullptr; // 通信器
  thread *thread_ = nullptr; // 线程对象
  volatile bool running_ = true; // 运行标志
};

// 单线程处理每个连接的线程处理器类
OneThreadPerConnectionThreadHandler::~OneThreadPerConnectionThreadHandler()
{
  stop(); // 停止处理
  await_stop(); // 等待停止
}

// 处理新连接
RC OneThreadPerConnectionThreadHandler::new_connection(Communicator *communicator)
{
  lock_guard guard(lock_); // 获取锁

  auto iter = thread_map_.find(communicator); // 查找线程映射
  if (iter != thread_map_.end()) {
    LOG_WARN("connection already exists. communicator = %p", communicator);
    return RC::FILE_EXIST; // 连接已存在
  }

  Worker *worker = new Worker(*this, communicator); // 创建工作线程
  thread_map_[communicator] = worker; // 添加到线程映射
  return worker->start(); // 启动线程
}

// 关闭连接
RC OneThreadPerConnectionThreadHandler::close_connection(Communicator *communicator)
{
  lock_.lock(); // 获取锁
  auto iter = thread_map_.find(communicator); // 查找线程映射
  if (iter == thread_map_.end()) {
    LOG_WARN("connection not exists. communicator = %p", communicator);
    lock_.unlock(); // 释放锁
    return RC::FILE_NOT_EXIST; // 连接不存在
  }

  Worker *worker = iter->second; // 获取工作线程
  thread_map_.erase(iter); // 从线程映射中删除
  lock_.unlock(); // 释放锁

  worker->stop(); // 停止线程
  worker->join(); // 等待线程结束
  delete worker; // 删除工作线程
  delete communicator; // 删除通信器
  LOG_INFO("close connection. communicator = %p", communicator);
  return RC::SUCCESS; // 成功
}

// 停止处理
RC OneThreadPerConnectionThreadHandler::stop()
{
  lock_guard guard(lock_); // 获取锁
  for (auto iter = thread_map_.begin(); iter != thread_map_.end(); ++iter) {
    Worker *worker = iter->second; // 获取工作线程
    worker->stop(); // 停止线程
  }
  return RC::SUCCESS; // 成功
}

// 等待停止
RC OneThreadPerConnectionThreadHandler::await_stop()
{
  LOG_INFO("begin to await stop one thread per connection thread handler");
  while (!thread_map_.empty()) {
    this_thread::sleep_for(chrono::milliseconds(100)); // 等待100毫秒
  }
  LOG_INFO("end to await stop one thread per connection thread handler");
  return RC::SUCCESS; // 成功
}