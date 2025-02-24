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
#include <string.h>  // 提供字符串处理函数的库，如strcasecmp

#include "net/thread_handler.h"  // ThreadHandler类的声明
#include "net/one_thread_per_connection_thread_handler.h"  // OneThreadPerConnectionThreadHandler类的声明
#include "net/java_thread_pool_thread_handler.h"  // JavaThreadPoolThreadHandler类的声明
#include "common/log/log.h"  // 日志记录功能的声明
#include "common/lang/string.h"  // 提供字符串处理功能的声明

// ThreadHandler类的成员函数create的定义
// 该函数用于根据传入的名称创建并返回一个具体的线程处理器对象
ThreadHandler * ThreadHandler::create(const char *name)
{
  // 默认的线程处理器名称
  const char *default_name = "one-thread-per-connection";
  
  // 如果传入的名称为空或者为空字符串，则使用默认名称
  if (nullptr == name || common::is_blank(name)) {
    name = default_name;
  }

  // 使用strcasecmp函数比较名称是否为"one-thread-per-connection"（不区分大小写）
  if (0 == strcasecmp(name, default_name)) {
    // 如果匹配，则创建并返回一个OneThreadPerConnectionThreadHandler对象
    return new OneThreadPerConnectionThreadHandler();
  } 
  // 使用strcasecmp函数比较名称是否为"java-thread-pool"（不区分大小写）
  else if (0 == strcasecmp(name, "java-thread-pool")) {
    // 如果匹配，则创建并返回一个JavaThreadPoolThreadHandler对象
    return new JavaThreadPoolThreadHandler();
  } 
  else {
    // 如果名称不匹配任何已知的线程处理器，则记录错误日志，并返回空指针
    LOG_ERROR("unknown thread handler: %s", name);
    return nullptr;
  }
}