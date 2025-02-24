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
// 包含所需的头文件
#include "net/thread_handler.h"  // 包含线程处理的基类定义
#include "common/lang/mutex.h"    // 包含互斥锁的定义
#include "common/lang/unordered_map.h"  // 包含无序映射容器的定义

class Worker;  // 声明一个Worker类，这个类在代码中没有给出，可能是用于处理每个连接的线程的工作

/**
 * @brief 一个连接一个线程的线程模型
 * @ingroup ThreadHandler
 */
class OneThreadPerConnectionThreadHandler : public ThreadHandler
{
public:
  // 构造函数
  OneThreadPerConnectionThreadHandler() = default;
  
  // 析构函数
  virtual ~OneThreadPerConnectionThreadHandler();

  // 启动线程处理器
  //! @copydoc ThreadHandler::start
  virtual RC start() override { return RC::SUCCESS; }

  // 停止线程处理器
  //! @copydoc ThreadHandler::stop
  virtual RC stop() override;

  // 等待线程处理器停止
  //! @copydoc ThreadHandler::await_stop
  virtual RC await_stop() override;

  // 处理新的连接
  //! @copydoc ThreadHandler::new_connection
  virtual RC new_connection(Communicator *communicator) override;

  // 关闭连接
  //! @copydoc ThreadHandler::close_connection
  virtual RC close_connection(Communicator *communicator) override;

private:
  /// 记录一个连接Communicator关联的线程数据
  unordered_map<Communicator *, Worker *> thread_map_;  // 使用无序映射容器来存储Communicator和Worker的关联关系
  
  /// 保护线程安全的锁
  mutex lock_;  // 使用互斥锁来保护共享资源的访问
};