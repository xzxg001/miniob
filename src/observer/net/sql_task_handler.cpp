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
#include "net/sql_task_handler.h"
#include "net/communicator.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"

// SqlTaskHandler 类的成员函数 handle_event 的定义
// 该函数负责处理来自 Communicator 的事件
RC SqlTaskHandler::handle_event(Communicator *communicator)
{
  // 定义一个 SessionEvent 类型的指针，用于接收事件
  SessionEvent *event = nullptr;
  
  // 从 Communicator 读取事件
  RC rc = communicator->read_event(event);
  // 如果读取失败，返回错误码
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 如果没有事件，直接返回成功
  if (nullptr == event) {
    return RC::SUCCESS;
  }

  // 处理接收到的请求
  session_stage_.handle_request2(event);

  // 创建一个 SQLStageEvent 对象，用于封装 SQL 事件和查询
  SQLStageEvent sql_event(event, event->query());

  // 处理 SQL 事件
  rc = handle_sql(&sql_event);
  // 如果处理失败，记录错误码到事件的 SQL 结果中
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to handle sql. rc=%s", strrc(rc));
    event->sql_result()->set_return_code(rc);
  }

  // 初始化一个布尔值，用于标记是否需要断开连接
  bool need_disconnect = false;

  // 将结果写回 Communicator，并根据需要设置是否断开连接
  rc = communicator->write_result(event, need_disconnect);
  // 记录写回结果的状态
  LOG_INFO("write result return %s", strrc(rc));
  // 清除当前请求和会话
  event->session()->set_current_request(nullptr);
  Session::set_current_session(nullptr);

  // 删除事件对象，释放内存
  delete event;

  // 如果需要断开连接，返回内部错误码
  if (need_disconnect) {
    return RC::INTERNAL;
  }
  // 返回成功
  return RC::SUCCESS;
}

// SqlTaskHandler 类的成员函数 handle_sql 的定义
// 该函数负责处理 SQL 事件
RC SqlTaskHandler::handle_sql(SQLStageEvent *sql_event)
{
  // 处理查询缓存请求
  RC rc = query_cache_stage_.handle_request(sql_event);
  // 如果处理失败，记录并返回错误码
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  // 处理解析请求
  rc = parse_stage_.handle_request(sql_event);
  // 如果处理失败，记录并返回错误码
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  // 处理解析请求
  rc = resolve_stage_.handle_request(sql_event);
  // 如果处理失败，记录并返回错误码
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }

  // 处理优化请求
  rc = optimize_stage_.handle_request(sql_event);
  // 如果处理失败，并且错误码既不是未实现也不是成功，则记录并返回错误码
  if (rc != RC::UNIMPLEMENTED && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }

  // 执行 SQL 事件
  rc = execute_stage_.handle_request(sql_event);
  // 如果执行失败，记录并返回错误码
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  // 返回处理结果
  return rc;
}