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
// Created by Wangyunlai on 2023/6/29.
//

#pragma once  // 预处理指令，确保头文件只被包含一次

#include <string>  // 引入字符串库
#include <vector>  // 引入向量容器库

#include "sql/stmt/stmt.h"  // 引入基类Stmt的声明

/**
 * @brief SetVariableStmt 语句，设置变量，当前是会话变量，但是只有会话变量，没有全局变量
 * @ingroup Statement
 */
class SetVariableStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化设置变量的SQL节点
  SetVariableStmt(const SetVariableSqlNode &set_variable) : set_variable_(set_variable) {}
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~SetVariableStmt() = default;

  // 返回语句类型，这里是设置变量
  StmtType type() const override { return StmtType::SET_VARIABLE; }

  // 获取变量名的访问器
  const char  *var_name() const { return set_variable_.name.c_str(); }
  
  // 获取变量值的访问器
  const Value &var_value() const { return set_variable_.value; }

  // 静态方法，用于创建SetVariableStmt对象
  static RC create(const SetVariableSqlNode &set_variable, Stmt *&stmt)
  {
    /// 可以校验是否存在某个变量，但是这里忽略
    stmt = new SetVariableStmt(set_variable);  // 创建SetVariableStmt对象
    return RC::SUCCESS;  // 返回成功
  }

private:
  SetVariableSqlNode set_variable_;  // 存储设置变量的SQL节点
};