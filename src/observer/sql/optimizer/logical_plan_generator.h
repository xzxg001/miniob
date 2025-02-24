/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//
#pragma once  // 预处理指令，确保头文件只被包含一次

#include <memory>  // 包含C++标准库中的智能指针支持

#include "common/rc.h"  // 包含自定义的返回码定义
#include "common/type/attr_type.h"  // 包含自定义的数据类型定义

// 声明相关的类，这些类的定义在其他头文件中
class Stmt;
class CalcStmt;
class SelectStmt;
class FilterStmt;
class InsertStmt;
class DeleteStmt;
class ExplainStmt;
class LogicalOperator;

// LogicalPlanGenerator类用于生成SQL查询的逻辑计划
class LogicalPlanGenerator
{
public:
  // 默认构造函数
  LogicalPlanGenerator() = default;
  // 默认虚析构函数，允许派生类的析构函数被正确调用
  virtual ~LogicalPlanGenerator() = default;

  // create函数用于根据SQL语句生成逻辑操作符
  RC create(Stmt *stmt, std::unique_ptr<LogicalOperator> &logical_operator);

private:
  // create_plan函数用于根据不同类型的SQL语句生成逻辑操作符
  RC create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(SelectStmt *select_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(FilterStmt *filter_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(InsertStmt *insert_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(DeleteStmt *delete_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(ExplainStmt *explain_stmt, std::unique_ptr<LogicalOperator> &logical_operator);

  // create_group_by_plan函数用于根据选择语句生成GROUP BY逻辑操作符
  RC create_group_by_plan(SelectStmt *select_stmt, std::unique_ptr<LogicalOperator> &logical_operator);

  // implicit_cast_cost函数用于计算从一种数据类型隐式转换到另一种数据类型的成本
  int implicit_cast_cost(AttrType from, AttrType to);
};