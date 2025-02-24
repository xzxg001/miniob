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

#include <string.h>
#include <string>  // 包含标准C++字符串库

#include "resolve_stage.h"  // 包含ResolveStage类的定义

// 包含其他依赖的头文件
#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/stmt.h"

using namespace common;  // 使用common命名空间，简化代码

RC ResolveStage::handle_request(SQLStageEvent *sql_event)
{
  RC            rc            = RC::SUCCESS;  // 初始化返回码为成功
  SessionEvent *session_event = sql_event->session_event();  // 获取SQL事件中的会话事件
  SqlResult    *sql_result    = session_event->sql_result();  // 获取SQL结果对象

  // 获取当前会话的数据库对象
  Db *db = session_event->session()->get_current_db();
  if (nullptr == db) {
    LOG_ERROR("cannot find current db");  // 如果数据库对象为空，记录错误日志
    rc = RC::SCHEMA_DB_NOT_EXIST;  // 设置返回码为数据库不存在
    sql_result->set_return_code(rc);  // 设置SQL结果的返回码
    sql_result->set_state_string("no db selected");  // 设置SQL结果的状态字符串
    return rc;  // 返回错误码
  }

  // 获取解析后的SQL语句节点
  ParsedSqlNode *sql_node = sql_event->sql_node().get();
  Stmt          *stmt     = nullptr;  // 初始化语句对象为nullptr

  // 根据解析后的SQL语句节点创建对应的语句对象
  rc = Stmt::create_stmt(db, *sql_node, stmt);
  if (rc != RC::SUCCESS && rc != RC::UNIMPLEMENTED) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));  // 如果创建失败且不是未实现，记录警告日志
    sql_result->set_return_code(rc);  // 设置SQL结果的返回码
    return rc;  // 返回错误码
  }

  // 将创建的语句对象设置到SQL事件中
  sql_event->set_stmt(stmt);

  return rc;  // 返回操作结果
}
