/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
您可以根据 Mulan PSL v2 的条款和条件使用本软件。
您可以在以下网址获取 Mulan PSL v2 的副本：
         http://license.coscl.org.cn/MulanPSL2
本软件是按“原样”提供的，不提供任何类型的保证，
无论是明示还是暗示，包括但不限于不侵权、
适销性或特定用途的适用性。
有关详细信息，请参阅 Mulan PSL v2。
*/
// Created by Wangyunlai on 2023/6/29.
#include <stdarg.h>               // 引入可变参数相关的头文件
#include "event/session_event.h"  // 引入 SessionEvent 类的头文件
#include "event/sql_debug.h"      // 引入 SqlDebug 类的头文件
#include "session/session.h"      // 引入 Session 类的头文件

/**
 * @brief 向 SQL 调试信息中添加调试信息
 *
 * @param debug_info 要添加的调试信息字符串
 */
void SqlDebug::add_debug_info(const string &debug_info)
{
  debug_infos_.push_back(debug_info);  // 将调试信息添加到列表中
}

/**
 * @brief 清除所有调试信息
 *
 * 清空当前存储的调试信息列表。
 */
void SqlDebug::clear_debug_info()
{
  debug_infos_.clear();  // 清空调试信息列表
}

/**
 * @brief 获取当前的调试信息列表
 *
 * @return 返回当前存储的调试信息列表的常量引用
 */
const list<string> &SqlDebug::get_debug_infos() const
{
  return debug_infos_;  // 返回调试信息列表
}

/**
 * @brief 添加 SQL 调试信息
 *
 * 此函数用于格式化传入的调试信息并将其添加到当前会话的调试信息列表中。
 *
 * @param fmt 格式化字符串
 * @param ... 格式化字符串后面的可变参数
 */
void sql_debug(const char *fmt, ...)
{
  Session *session = Session::current_session();  // 获取当前会话
  if (nullptr == session) {                       // 检查会话是否有效
    return;                                       // 如果无效，直接返回
  }

  SessionEvent *request = session->current_request();  // 获取当前请求
  if (nullptr == request) {                            // 检查请求是否有效
    return;                                            // 如果无效，直接返回
  }

  SqlDebug &sql_debug = request->sql_debug();  // 获取当前请求的 SQL 调试对象

  const int buffer_size = 4096;                   // 定义缓冲区大小
  char     *str         = new char[buffer_size];  // 动态分配缓冲区

  va_list ap;                            // 定义可变参数列表
  va_start(ap, fmt);                     // 初始化可变参数列表
  vsnprintf(str, buffer_size, fmt, ap);  // 格式化字符串
  va_end(ap);                            // 结束可变参数处理

  sql_debug.add_debug_info(str);          // 将格式化的调试信息添加到 SQL 调试信息中
  LOG_DEBUG("sql debug info: [%s]", str);  // 打印调试信息到日志中

  delete[] str;  // 释放动态分配的内存
}
