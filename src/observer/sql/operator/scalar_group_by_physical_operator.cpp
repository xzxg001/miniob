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

#include "common/log/log.h"                                  // 日志模块
#include "sql/operator/scalar_group_by_physical_operator.h"  // 头文件
#include "sql/expr/expression_tuple.h"                       // 表达式元组
#include "sql/expr/composite_tuple.h"                        // 复合元组

using namespace std;
using namespace common;

// 构造函数，接受一组表达式
ScalarGroupByPhysicalOperator::ScalarGroupByPhysicalOperator(vector<Expression *> &&expressions)
    : GroupByPhysicalOperator(std::move(expressions))
{}

// 打开物理算子并准备数据
RC ScalarGroupByPhysicalOperator::open(Trx *trx)
{
  // 确保子算子数量为1
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];    // 获取子算子
  RC                rc    = child.open(trx);  // 打开子算子
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));  // 日志记录失败
    return rc;
  }

  // 创建用于存储分组值的表达式元组
  ExpressionTuple<Expression *> group_value_expression_tuple(value_expressions_);
  ValueListTuple                group_by_evaluated_tuple;  // 用于存储评估后的值

  // 迭代子算子以获取每个元组
  while (OB_SUCC(rc = child.next())) {
    Tuple *child_tuple = child.current_tuple();  // 获取当前元组
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));  // 日志记录错误
      return RC::INTERNAL;                                                   // 返回内部错误
    }

    // 计算需要做聚合的值
    group_value_expression_tuple.set_tuple(child_tuple);

    // 如果当前没有分组值，创建新的聚合器
    if (group_value_ == nullptr) {
      AggregatorList aggregator_list;
      create_aggregator_list(aggregator_list);  // 创建聚合器列表

      ValueListTuple child_tuple_to_value;                            // 用于将元组转换为值列表
      rc = ValueListTuple::make(*child_tuple, child_tuple_to_value);  // 转换元组
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));  // 日志记录错误
        return rc;
      }

      CompositeTuple composite_tuple;                                                           // 复合元组
      composite_tuple.add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));  // 添加转换后的元组
      group_value_ =
          make_unique<GroupValueType>(std::move(aggregator_list), std::move(composite_tuple));  // 创建新的分组值
    }

    // 执行聚合操作
    rc = aggregate(get<0>(*group_value_), group_value_expression_tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to aggregate values. rc=%s", strrc(rc));  // 日志记录错误
      return rc;
    }
  }

  // 检查是否到达记录末尾
  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;  // 将状态改为成功
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));  // 日志记录错误
    return rc;
  }

  // 得到最终聚合后的值
  if (group_value_) {
    rc = evaluate(*group_value_);  // 评估聚合结果
  }

  emitted_ = false;  // 重置已输出标识
  return rc;         // 返回状态
}

// 获取下一个结果
RC ScalarGroupByPhysicalOperator::next()
{
  // 如果没有分组值或已经输出过，返回记录末尾
  if (group_value_ == nullptr || emitted_) {
    return RC::RECORD_EOF;
  }

  emitted_ = true;     // 标记为已输出
  return RC::SUCCESS;  // 返回成功状态
}

// 关闭物理算子并释放资源
RC ScalarGroupByPhysicalOperator::close()
{
  group_value_.reset();  // 重置分组值
  emitted_ = false;      // 重置已输出标识
  return RC::SUCCESS;    // 返回成功状态
}

// 获取当前元组
Tuple *ScalarGroupByPhysicalOperator::current_tuple()
{
  if (group_value_ == nullptr) {
    return nullptr;  // 如果没有分组值，返回空
  }

  return &get<1>(*group_value_);  // 返回当前聚合后的元组
}
