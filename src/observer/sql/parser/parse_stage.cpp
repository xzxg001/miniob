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

#include "parse_stage.h"  // 包含ParseStage类的定义

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/parser/parse.h"  // 包含SQL解析函数的声明

using namespace common;  // 使用common命名空间，简化代码

RC ParseStage::handle_request(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 从SQL事件中获取SqlResult对象和SQL语句字符串
  SqlResult *sql_result = sql_event->session_event()->sql_result();
  const std::string &sql = sql_event->sql();

  // 创建一个ParsedSqlResult对象来存储解析结果
  ParsedSqlResult parsed_sql_result;

  // 调用parse函数进行SQL解析
  parse(sql.c_str(), &parsed_sql_result);
  if (parsed_sql_result.sql_nodes().empty()) {
    sql_result->set_return_code(RC::SUCCESS);  // 设置返回码为成功
    sql_result->set_state_string("");  // 设置状态字符串为空
    return RC::INTERNAL;  // 如果没有解析出任何SQL节点，返回内部错误
  }

  // 如果解析出多个SQL命令，记录警告日志
  if (parsed_sql_result.sql_nodes().size() > 1) {
    LOG_WARN("got multi sql commands but only 1 will be handled");
  }

  // 从解析结果中取出第一个SQL节点
  std::unique_ptr<ParsedSqlNode> sql_node = std::move(parsed_sql_result.sql_nodes().front());
  if (sql_node->flag == SCF_ERROR) {
    // 如果解析标志为错误，设置错误信息到事件
    rc = RC::SQL_SYNTAX;  // 设置返回码为SQL语法错误
    sql_result->set_return_code(rc);  // 设置SqlResult的返回码
    sql_result->set_state_string("Failed to parse sql");  // 设置状态字符串
    return rc;  // 返回错误码
  }

  // 将解析出的SQL节点设置到SQL事件中
  sql_event->set_sql_node(std::move(sql_node));

  return RC::SUCCESS;  // 返回成功状态码
}