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
// Created by Wangyunlai on 2023/6/14.
//
// 确保头文件只被包含一次
#pragma once

// 引入必要的头文件，这些头文件提供了类所需的支持
#include "common/rc.h"  // 提供返回码RC的定义
#include "event/session_event.h"  // 提供SessionEvent的定义
#include "event/sql_event.h"  // 提供SQLStageEvent的定义
#include "session/session.h"  // 提供Session的定义
#include "sql/stmt/set_variable_stmt.h"  // 提供SetVariableStmt的定义

/**
 * @brief SetVariable语句执行器
 * @ingroup Executor
 */
class SetVariableExecutor
{
public:
  // 默认构造函数
  SetVariableExecutor()          = default;
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~SetVariableExecutor() = default;

  // execute函数用于执行SET语句
  RC execute(SQLStageEvent *sql_event);

private:
  // var_value_to_boolean函数用于将Value类型的值转换为布尔型
  RC var_value_to_boolean(const Value &var_value, bool &bool_value) const;

  // get_execution_mode函数用于从Value中获取执行模式
  RC get_execution_mode(const Value &var_value, ExecutionMode &execution_mode) const;
};