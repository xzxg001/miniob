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
// Created by Wangyunlai on 2022/12/14.
//

#pragma once  // 预处理指令，确保头文件只被包含一次

#include "common/rc.h"  // 包含自定义的返回码定义
#include "sql/operator/logical_operator.h"  // 包含逻辑操作符的头文件
#include "sql/operator/physical_operator.h"  // 包含物理操作符的头文件

// 声明相关的逻辑操作符类，这些类的定义在其他头文件中
class TableGetLogicalOperator;
class PredicateLogicalOperator;
class ProjectLogicalOperator;
class InsertLogicalOperator;
class DeleteLogicalOperator;
class ExplainLogicalOperator;
class JoinLogicalOperator;
class CalcLogicalOperator;
class GroupByLogicalOperator;

/**
 * @brief 物理计划生成器
 * @ingroup PhysicalOperator
 * @details 根据逻辑计划生成物理计划。
 * 不会做任何优化，完全根据本意生成物理计划。
 */
class PhysicalPlanGenerator
{
public:
  // 默认构造函数
  PhysicalPlanGenerator() = default;
  // 默认虚析构函数，允许派生类的析构函数被正确调用
  virtual ~PhysicalPlanGenerator() = default;

  // create函数用于根据逻辑操作符生成物理操作符
  RC create(LogicalOperator &logical_operator, std::unique_ptr<PhysicalOperator> &oper);
  // create_vec函数用于根据逻辑操作符生成向量化物理操作符
  RC create_vec(LogicalOperator &logical_operator, std::unique_ptr<PhysicalOperator> &oper);

private:
  // create_plan函数用于根据不同类型的逻辑操作符生成物理操作符
  RC create_plan(TableGetLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(PredicateLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(ProjectLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(InsertLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(DeleteLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(ExplainLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(JoinLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(CalcLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_plan(GroupByLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  
  // create_vec_plan函数用于根据不同类型的逻辑操作符生成向量化物理操作符
  RC create_vec_plan(ProjectLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_vec_plan(TableGetLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_vec_plan(GroupByLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
  RC create_vec_plan(ExplainLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper);
};
