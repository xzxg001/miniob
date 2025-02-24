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

#include "sql/operator/physical_operator.h"  // 引入物理算子的基类
#include "sql/expr/composite_tuple.h"        // 引入复合元组的定义

/**
 * @brief Group By 物理算子基类
 * @ingroup PhysicalOperator
 */
class GroupByPhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief 构造函数
   *
   * @param expressions 聚合表达式列表
   */
  GroupByPhysicalOperator(std::vector<Expression *> &&expressions);

  // 默认析构函数
  virtual ~GroupByPhysicalOperator() = default;

protected:
  using AggregatorList = std::vector<std::unique_ptr<Aggregator>>;

  /**
   * @brief 聚合出来的一组数据
   * @details
   * 第一个参数是聚合函数列表，比如需要计算 sum(a), avg(b), count(c)。
   * 第二个参数是聚合的最终结果，它也包含两个元素：
   *   1. 缓存下来的元组
   *   2. 聚合函数计算的结果。
   *
   * 之所以要缓存下来一个元组，是要解决以下问题：
   * select a, b, sum(a) from t group by a;
   * 我们需要知道 b 的值是什么，虽然它不确定。
   */
  using GroupValueType = std::tuple<AggregatorList, CompositeTuple>;

protected:
  /**
   * @brief 创建聚合器列表
   *
   * @param aggregator_list 将生成的聚合器列表
   */
  void create_aggregator_list(AggregatorList &aggregator_list);

  /**
   * @brief 聚合一条记录
   *
   * @param aggregator_list 需要执行聚合运算的列表
   * @param tuple 执行聚合运算的一条记录
   * @return 返回聚合操作的结果状态
   */
  RC aggregate(AggregatorList &aggregator_list, const Tuple &tuple);

  /**
   * @brief 所有元组聚合结束后，运算最终结果
   *
   * @param group_value 聚合后的结果
   * @return 返回评估操作的结果状态
   */
  RC evaluate(GroupValueType &group_value);

protected:
  std::vector<Expression *> aggregate_expressions_;  /// 聚合表达式
  std::vector<Expression *> value_expressions_;      /// 计算聚合时的表达式
};
