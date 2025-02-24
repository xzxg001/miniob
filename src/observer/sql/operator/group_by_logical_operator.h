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
// Created by WangYunlai on 2024/05/29.
//

#pragma once

#include "sql/operator/logical_operator.h"  // 引入逻辑算子的头文件

/**
 * @brief GroupBy 逻辑算子
 *
 * 该类表示 SQL 查询中的 GROUP BY 操作逻辑。
 */
class GroupByLogicalOperator : public LogicalOperator
{
public:
  /**
   * @brief 构造函数
   *
   * @param group_by_exprs 用于分组的表达式列表
   * @param expressions 输出的表达式列表，可能包含聚合函数
   */
  GroupByLogicalOperator(std::vector<std::unique_ptr<Expression>> &&group_by_exprs,  // 分组表达式
      std::vector<Expression *>                                   &&expressions      // 输出表达式
  );

  virtual ~GroupByLogicalOperator() = default;  // 默认析构函数

  /**
   * @brief 返回逻辑算子的类型
   *
   * @return LogicalOperatorType 逻辑算子类型
   */
  LogicalOperatorType type() const override { return LogicalOperatorType::GROUP_BY; }

  /**
   * @brief 获取分组表达式列表
   *
   * @return 分组表达式列表的引用
   */
  auto &group_by_expressions() { return group_by_expressions_; }

  /**
   * @brief 获取聚合表达式列表
   *
   * @return 聚合表达式列表的引用
   */
  auto &aggregate_expressions() { return aggregate_expressions_; }

private:
  std::vector<std::unique_ptr<Expression>> group_by_expressions_;   // 存储分组表达式的向量
  std::vector<Expression *>                aggregate_expressions_;  ///< 输出的表达式，可能包含聚合函数
};
