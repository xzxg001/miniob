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
// 包含标准库头文件
#include <sstream>
#include <string>

// 包含执行阶段的头文件
#include "sql/executor/execute_stage.h"

// 包含日志模块的头文件
#include "common/log/log.h"
// 包含会话事件的头文件
#include "event/session_event.h"
// 包含SQL事件的头文件
#include "event/sql_event.h"
// 包含命令执行器的头文件
#include "sql/executor/command_executor.h"
// 包含计算物理操作符的头文件
#include "sql/operator/calc_physical_operator.h"
// 包含选择语句的头文件
#include "sql/stmt/select_stmt.h"
// 包含语句的头文件
#include "sql/stmt/stmt.h"
// 包含默认处理器的头文件
#include "storage/default/default_handler.h"

// 使用标准命名空间
using namespace std;
using namespace common;

// ExecuteStage类用于处理SQL请求
class ExecuteStage
{
public:
  // handle_request方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC handle_request(SQLStageEvent *sql_event)
  {
    // 初始化返回码为成功
    RC rc = RC::SUCCESS;

    // 获取物理操作符
    const unique_ptr<PhysicalOperator> &physical_operator = sql_event->physical_operator();
    // 如果物理操作符不为空，则使用它来处理请求
    if (physical_operator != nullptr) {
      return handle_request_with_physical_operator(sql_event);
    }

    // 从事件中获取会话事件对象
    SessionEvent *session_event = sql_event->session_event();

    // 从事件中获取SQL语句对象
    Stmt *stmt = sql_event->stmt();
    // 如果SQL语句对象不为空，则使用命令执行器来执行它
    if (stmt != nullptr) {
      CommandExecutor command_executor;
      rc = command_executor.execute(sql_event);
      session_event->sql_result()->set_return_code(rc);
    } else {
      // 如果SQL语句对象为空，则返回内部错误
      return RC::INTERNAL;
    }
    // 返回处理结果
    return rc;
  }

  // handle_request_with_physical_operator方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC handle_request_with_physical_operator(SQLStageEvent *sql_event)
  {
    // 初始化返回码为成功
    RC rc = RC::SUCCESS;

    // 获取物理操作符的引用
    unique_ptr<PhysicalOperator> &physical_operator = sql_event->physical_operator();
    // 断言物理操作符不为空
    ASSERT(physical_operator != nullptr, "physical operator should not be null");

    // 从会话事件中获取SQL结果对象
    SqlResult *sql_result = sql_event->session_event()->sql_result();
    // 将物理操作符设置到SQL结果中
    sql_result->set_operator(std::move(physical_operator));
    // 返回处理结果
    return rc;
  }
};