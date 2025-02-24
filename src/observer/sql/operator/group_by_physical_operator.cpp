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
// Created by WangYunlai on 2024/06/11.
//

#include <algorithm>  // 引入算法库

#include "common/log/log.h"                           // 日志模块
#include "sql/operator/group_by_physical_operator.h"  // GroupBy物理算子头文件
#include "sql/expr/expression_tuple.h"                // 表达式元组头文件
#include "sql/expr/composite_tuple.h"                 // 复合元组头文件

using namespace std;
using namespace common;

// 构造函数，接收聚合表达式
GroupByPhysicalOperator::GroupByPhysicalOperator(vector<Expression *> &&expressions)
{
  aggregate_expressions_ = std::move(expressions);            // 移动聚合表达式到成员变量中
  value_expressions_.reserve(aggregate_expressions_.size());  // 预留空间以存储值表达式

  // 遍历聚合表达式，将子表达式添加到值表达式列表
  ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
    value_expressions_.emplace_back(child_expr);  // 将子表达式加入值表达式列表
  });
}

// 创建聚合器列表
void GroupByPhysicalOperator::create_aggregator_list(AggregatorList &aggregator_list)
{
  aggregator_list.clear();                                 // 清空已有的聚合器列表
  aggregator_list.reserve(aggregate_expressions_.size());  // 预留空间以存储聚合器

  // 遍历聚合表达式，创建并添加聚合器
  ranges::for_each(aggregate_expressions_, [&aggregator_list](Expression *expr) {
    auto *aggregate_expr = static_cast<AggregateExpr *>(expr);
    aggregator_list.emplace_back(aggregate_expr->create_aggregator());  // 创建聚合器并添加到列表
  });
}

// 对一条记录执行聚合操作
RC GroupByPhysicalOperator::aggregate(AggregatorList &aggregator_list, const Tuple &tuple)
{
  // 确保聚合器列表的大小与元组的大小一致
  ASSERT(static_cast<int>(aggregator_list.size()) == tuple.cell_num(), 
         "aggregator list size must be equal to tuple size. aggregator num: %d, tuple num: %d",
         aggregator_list.size(), tuple.cell_num());

  RC        rc = RC::SUCCESS;  // 初始化返回代码
  Value     value;             // 用于存储从元组中提取的值
  const int size = static_cast<int>(aggregator_list.size());

  // 遍历聚合器，提取元组值并进行聚合
  for (int i = 0; i < size; i++) {
    Aggregator *aggregator = aggregator_list[i].get();  // 获取当前聚合器

    rc = tuple.cell_at(i, value);  // 从元组中提取值
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value from expression. rc=%s", strrc(rc));
      return rc;  // 提取失败，返回错误代码
    }

    rc = aggregator->accumulate(value);  // 将值传递给聚合器进行累积
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to accumulate value. rc=%s", strrc(rc));
      return rc;  // 累积失败，返回错误代码
    }
  }

  return rc;  // 返回成功代码
}

// 在所有元组聚合结束后，评估最终结果
RC GroupByPhysicalOperator::evaluate(GroupValueType &group_value)
{
  RC rc = RC::SUCCESS;  // 初始化返回代码

  vector<TupleCellSpec> aggregator_names;  // 存储聚合器名称
  // 遍历聚合表达式，将其名称添加到名称列表中
  for (Expression *expr : aggregate_expressions_) {
    aggregator_names.emplace_back(expr->name());
  }

  AggregatorList &aggregators           = get<0>(group_value);  // 获取聚合器列表
  CompositeTuple &composite_value_tuple = get<1>(group_value);  // 获取复合元组

  ValueListTuple evaluated_tuple;  // 用于存储评估后的元组
  vector<Value>  values;           // 存储聚合器评估结果的值

  // 遍历聚合器，评估每个聚合器并存储结果
  for (unique_ptr<Aggregator> &aggregator : aggregators) {
    Value value;                       // 存储聚合结果
    rc = aggregator->evaluate(value);  // 评估聚合器
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to evaluate aggregator. rc=%s", strrc(rc));
      return rc;  // 评估失败，返回错误代码
    }
    values.emplace_back(value);  // 将评估结果添加到值列表
  }

  // 设置评估元组的值和名称
  evaluated_tuple.set_cells(values);
  evaluated_tuple.set_names(aggregator_names);

  // 将评估后的元组添加到复合元组中
  composite_value_tuple.add_tuple(make_unique<ValueListTuple>(std::move(evaluated_tuple)));

  return rc;  // 返回成功代码
}
