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
// Created by WangYunlai on 2024/05/30.
//

#include <memory>  // 引入内存管理库

#include "common/log/log.h"                          // 引入日志库
#include "sql/operator/group_by_logical_operator.h"  // 引入 GroupBy 逻辑算子的头文件
#include "sql/expr/expression.h"                     // 引入表达式相关的头文件

using namespace std;  // 使用标准命名空间

/**
 * @brief GroupByLogicalOperator 构造函数
 *
 * @param group_by_exprs 用于分组的表达式列表
 * @param expressions 输出的表达式列表，可能包含聚合函数
 */
GroupByLogicalOperator::GroupByLogicalOperator(
    vector<unique_ptr<Expression>> &&group_by_exprs, vector<Expression *> &&expressions)
{
  // 使用 std::move 将传入的分组表达式移动到成员变量中，以避免不必要的拷贝
  group_by_expressions_ = std::move(group_by_exprs);

  // 使用 std::move 将传入的聚合表达式移动到成员变量中
  aggregate_expressions_ = std::move(expressions);
}
