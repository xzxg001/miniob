/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// 引入必要的头文件
#include "sql/executor/set_variable_executor.h"

// execute函数用于执行设置变量的操作
RC SetVariableExecutor::execute(SQLStageEvent *sql_event)
{
    RC rc = RC::SUCCESS;  // 初始化返回码为成功

    // 从sql_event中获取会话和待设置的变量语句
    Session         *session = sql_event->session_event()->session();
    SetVariableStmt *stmt    = (SetVariableStmt *)sql_event->stmt();

    // 获取变量名和值
    const char  *var_name  = stmt->var_name();
    const Value &var_value = stmt->var_value();
    
    // 根据变量名处理不同的设置
    if (strcasecmp(var_name, "sql_debug") == 0) {
      bool bool_value = false;
      // 将值转换为布尔型
      rc              = var_value_to_boolean(var_value, bool_value);
      if (rc == RC::SUCCESS) {
        // 设置会话的sql_debug标志
        session->set_sql_debug(bool_value);
        LOG_TRACE("set sql_debug to %d", bool_value);
      }
    } else if (strcasecmp(var_name, "execution_mode") == 0) {
      ExecutionMode  execution_mode = ExecutionMode::UNKNOWN_MODE;
      // 获取执行模式
      rc = get_execution_mode(var_value, execution_mode);
      if (rc == RC::SUCCESS && execution_mode != ExecutionMode::UNKNOWN_MODE) {
        session->set_execution_mode(execution_mode);
      } else {
        rc = RC::INVALID_ARGUMENT;
      }
    } else {
      rc = RC::VARIABLE_NOT_EXISTS;  // 变量名不存在
    }

    return rc;  // 返回操作结果
}

// var_value_to_boolean函数用于将Value类型的值转换为布尔型
RC SetVariableExecutor::var_value_to_boolean(const Value &var_value, bool &bool_value) const
{
    RC rc = RC::SUCCESS;

    // 根据值的类型进行转换
    if (var_value.attr_type() == AttrType::BOOLEANS) {
      bool_value = var_value.get_boolean();
    } else if (var_value.attr_type() == AttrType::INTS) {
      bool_value = var_value.get_int() != 0;
    } else if (var_value.attr_type() == AttrType::FLOATS) {
      bool_value = var_value.get_float() != 0.0;
    } else if (var_value.attr_type() == AttrType::CHARS) {
      // 字符串类型的值需要特别处理，根据预定义的字符串确定布尔值
      std::string true_strings[] = {"true", "on", "yes", "t", "1"};
      std::string false_strings[] = {"false", "off", "no", "f", "0"};

      // 检查字符串是否表示真值
      for (size_t i = 0; i < sizeof(true_strings) / sizeof(true_strings[0]); i++) {
        if (strcasecmp(var_value.get_string().c_str(), true_strings[i].c_str()) == 0) {
          bool_value = true;
          return rc;
        }
      }

      // 检查字符串是否表示假值
      for (size_t i = 0; i < sizeof(false_strings) / sizeof(false_strings[0]); i++) {
        if (strcasecmp(var_value.get_string().c_str(), false_strings[i].c_str()) == 0) {
          bool_value = false;
          return rc;
        }
      }
      rc = RC::VARIABLE_NOT_VALID;  // 字符串不是有效的布尔表示
    }

    return rc;  // 返回操作结果
}

// get_execution_mode函数用于将Value类型的值转换为ExecutionMode枚举
RC SetVariableExecutor::get_execution_mode(const Value &var_value, ExecutionMode &execution_mode) const
{
    RC rc = RC::SUCCESS;
  
    // 仅当值为字符串类型时进行处理
    if (var_value.attr_type() == AttrType::CHARS) {
      // 检查字符串并设置相应的执行模式
      if (strcasecmp(var_value.get_string().c_str(), "TUPLE_ITERATOR") == 0) {
        execution_mode = ExecutionMode::TUPLE_ITERATOR;
      } else if (strcasecmp(var_value.get_string().c_str(), "CHUNK_ITERATOR") == 0) {
        execution_mode = ExecutionMode::CHUNK_ITERATOR;
      } else {
        execution_mode = ExecutionMode::UNKNOWN_MODE;
        rc = RC::VARIABLE_NOT_VALID;  // 字符串不是有效的执行模式
      }
    } else  {
      execution_mode = ExecutionMode::UNKNOWN_MODE;
      rc = RC::VARIABLE_NOT_VALID;  // 值不是字符串类型
    }

    return rc;  // 返回操作结果
}