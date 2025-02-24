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
// Created by Longda on 2021/4/13.
//

#include "session_stage.h"

#include <string.h>

#include "common/conf/ini.h"
#include "common/lang/mutex.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "net/communicator.h"
#include "net/server.h"
#include "session/session.h"

using namespace common;

// Destructor
SessionStage::~SessionStage() {}

// TODO remove me
/**
 * 处理会话阶段的请求
 * 
 * @param sev 指向会话事件的指针，包含会话信息和请求详情
 */
void SessionStage::handle_request(SessionEvent *sev)
{
  // 获取请求的SQL语句
  string sql = sev->query();
  // 如果SQL语句为空，则直接返回，不进行后续处理
  if (common::is_blank(sql.c_str())) {
    return;
  }

  // 设置当前会话，以便在后续处理中能够访问会话信息
  Session::set_current_session(sev->session());
  // 设置当前请求，以便在后续处理中能够访问请求信息
  sev->session()->set_current_request(sev);

  // 创建SQL事件，用于处理SQL请求
  SQLStageEvent sql_event(sev, sql);
  // 处理SQL事件，返回处理结果
  (void)handle_sql(&sql_event);

  // 获取通信器，用于与客户端通信
  Communicator *communicator    = sev->get_communicator();
  // 初始化是否需要断开连接的标志
  bool          need_disconnect = false;
  // 执行写入结果的操作，并记录操作结果
  RC            rc              = communicator->write_result(sev, need_disconnect);
  LOG_INFO("write result return %s", strrc(rc));

  // 根据need_disconnect的值决定是否需要断开连接
  if (need_disconnect) {
    // do nothing
  }

  // 清理当前请求和会话，为下一次请求处理做准备
  sev->session()->set_current_request(nullptr);
  Session::set_current_session(nullptr);
}

/**
 * 处理第二个请求阶段的函数
 * 该函数主要用于处理会话中的查询请求，进行SQL语句的提取和当前会话及请求的设置
 * @param event 会话事件对象指针，包含会话信息和查询请求
 */
void SessionStage::handle_request2(SessionEvent *event)
{
  // 获取会话事件中的SQL查询语句
  const string &sql = event->query();
  
  // 检查SQL语句是否为空，如果为空则直接返回，不进行后续处理
  if (common::is_blank(sql.c_str())) {
    return;
  }

  // 设置当前会话，以便在后续处理中能够访问会话相关信息
  Session::set_current_session(event->session());
  
  // 设置当前会话的当前请求，以便跟踪和管理会话中的请求
  event->session()->set_current_request(event);
  
  // 创建SQLStageEvent对象，用于后续的SQL处理阶段
  SQLStageEvent sql_event(event, sql);
}

/**
 * 处理一个SQL语句经历这几个阶段。
 * 虽然看起来流程比较多，但是对于大多数SQL来说，更多的可以关注parse和executor阶段。
 * 通常只有select、delete等带有查询条件的语句才需要进入optimize。
 * 对于DDL语句，比如create table、create index等，没有对应的查询计划，可以直接搜索
 * create_table_executor、create_index_executor来看具体的执行代码。
 * select、delete等DML语句，会产生一些执行计划，如果感觉繁琐，可以跳过optimize直接看
 * execute_stage中的执行，通过explain语句看需要哪些operator，然后找对应的operator来
 * 调试或者看代码执行过程即可。
 */
/**
 * 处理SQL查询的各个阶段
 *
 * 本函数负责按顺序调用SQL处理的各个阶段，包括查询缓存、解析、解析器、优化和执行
 * 如果任何阶段失败，函数将记录错误并返回相应的错误代码
 * 
 * @param sql_event 指向SQL阶段事件的指针，包含SQL查询的相关信息
 * @return RC 返回执行结果，成功或错误代码
 */
RC SessionStage::handle_sql(SQLStageEvent *sql_event)
{
  // 尝试在查询缓存阶段处理请求
  RC rc = query_cache_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  // 解析SQL请求
  rc = parse_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  // 解析器阶段处理请求
  rc = resolve_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }

  // 优化SQL请求，允许未实现的情况作为成功处理
  rc = optimize_stage_.handle_request(sql_event);
  if (rc != RC::UNIMPLEMENTED && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }

  // 执行SQL请求
  rc = execute_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  // 返回最终执行结果
  return rc;
}
