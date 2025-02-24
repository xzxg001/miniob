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

#include "common/log/log.h"                                // 引入日志模块
#include "sql/operator/hash_group_by_physical_operator.h"  // 引入 HashGroupByPhysicalOperator 的头文件
#include "sql/expr/expression_tuple.h"                     // 引入表达式元组的头文件
#include "sql/expr/composite_tuple.h"                      // 引入复合元组的头文件

using namespace std;     // 使用标准命名空间
using namespace common;  // 使用通用命名空间

// HashGroupByPhysicalOperator 的构造函数
HashGroupByPhysicalOperator::HashGroupByPhysicalOperator(
    vector<unique_ptr<Expression>> &&group_by_exprs,    // 分组表达式的唯一指针向量
    vector<Expression *>           &&expressions)                 // 聚合表达式的指针向量
    : GroupByPhysicalOperator(std::move(expressions)),  // 调用基类构造函数初始化
      group_by_exprs_(std::move(group_by_exprs))        // 初始化分组表达式
{}

// 打开操作符，初始化状态，接受事务指针
RC HashGroupByPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());  // 确保只有一个子操作符

  PhysicalOperator &child = *children_[0];                       // 获取子操作符
  RC                rc    = child.open(trx);                     // 打开子操作符并获取返回状态
  if (OB_FAIL(rc)) {                                             // 如果打开失败
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));  // 记录日志
    return rc;                                                   // 返回错误码
  }

  ExpressionTuple<Expression *> group_value_expression_tuple(value_expressions_);  // 创建聚合值表达式元组

  ValueListTuple group_by_evaluated_tuple;  // 存储分组计算结果的值元组

  while (OB_SUCC(rc = child.next())) {           // 迭代获取子操作符的下一行结果
    Tuple *child_tuple = child.current_tuple();  // 获取当前元组
    if (nullptr == child_tuple) {                // 如果元组为空
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));  // 记录警告
      return RC::INTERNAL;                                                   // 返回内部错误
    }

    // 找到对应的 group
    GroupType *found_group = nullptr;                                // 初始化找到的分组指针
    rc                     = find_group(*child_tuple, found_group);  // 查找当前元组对应的分组
    if (OB_FAIL(rc)) {                                               // 如果查找失败
      LOG_WARN("failed to find group. rc=%s", strrc(rc));             // 记录警告
      return rc;                                                     // 返回错误码
    }

    // 计算需要做聚合的值
    group_value_expression_tuple.set_tuple(child_tuple);  // 设置当前子元组

    // 计算聚合值
    GroupValueType &group_value = get<1>(*found_group);  // 获取找到的分组的聚合值
    rc                          = aggregate(get<0>(group_value), group_value_expression_tuple);  // 进行聚合操作
    if (OB_FAIL(rc)) {                                                                           // 如果聚合失败
      LOG_WARN("failed to aggregate values. rc=%s", strrc(rc));                                   // 记录警告
      return rc;                                                                                 // 返回错误码
    }
  }

  if (RC::RECORD_EOF == rc) {  // 如果已经读取到文件末尾
    rc = RC::SUCCESS;          // 设置返回状态为成功
  }

  if (OB_FAIL(rc)) {                                        // 如果读取失败
    LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));  // 记录警告
    return rc;                                              // 返回错误码
  }

  // 得到最终聚合后的值
  for (GroupType &group : groups_) {                                // 遍历所有分组
    GroupValueType &group_value = get<1>(group);                    // 获取每个分组的聚合值
    rc                          = evaluate(group_value);            // 评估分组的聚合值
    if (OB_FAIL(rc)) {                                              // 如果评估失败
      LOG_WARN("failed to evaluate group value. rc=%s", strrc(rc));  // 记录警告
      return rc;                                                    // 返回错误码
    }
  }

  current_group_ = groups_.begin();  // 重置当前分组迭代器
  first_emited_  = false;            // 重置输出标志
  return rc;                         // 返回状态码
}

// 获取下一行结果
RC HashGroupByPhysicalOperator::next()
{
  if (current_group_ == groups_.end()) {  // 如果当前分组已到达结束
    return RC::RECORD_EOF;                // 返回文件结束标志
  }

  if (first_emited_) {  // 如果已输出第一条数据
    ++current_group_;   // 移动到下一条分组
  } else {
    first_emited_ = true;  // 标记第一条数据已输出
  }
  if (current_group_ == groups_.end()) {  // 如果当前分组已到达结束
    return RC::RECORD_EOF;                // 返回文件结束标志
  }

  return RC::SUCCESS;  // 返回成功状态
}

// 关闭操作符，释放资源
RC HashGroupByPhysicalOperator::close()
{
  children_[0]->close();                // 关闭子操作符
  LOG_INFO("close group by operator");  // 记录关闭日志
  return RC::SUCCESS;                   // 返回成功状态
}

// 获取当前的元组
Tuple *HashGroupByPhysicalOperator::current_tuple()
{
  if (current_group_ != groups_.end()) {                    // 如果当前分组不为空
    GroupValueType &group_value = get<1>(*current_group_);  // 获取当前分组的聚合值
    return &get<1>(group_value);                            // 返回当前分组的元组
  }
  return nullptr;  // 如果没有当前分组，返回空指针
}

// 查找与给定元组相对应的分组
RC HashGroupByPhysicalOperator::find_group(const Tuple &child_tuple, GroupType *&found_group)
{
  found_group = nullptr;  // 初始化找到的分组指针为 null

  RC rc = RC::SUCCESS;  // 初始化返回状态

  ExpressionTuple<unique_ptr<Expression>> group_by_expression_tuple(group_by_exprs_);  // 创建分组表达式元组
  ValueListTuple                          group_by_evaluated_tuple;  // 存储分组计算结果的值元组
  group_by_expression_tuple.set_tuple(&child_tuple);                 // 设置元组为当前子元组
  rc = ValueListTuple::make(group_by_expression_tuple, group_by_evaluated_tuple);  // 从表达式元组生成值元组
  if (OB_FAIL(rc)) {                                                               // 如果生成失败
    LOG_WARN("failed to get values from expression tuple. rc=%s", strrc(rc));       // 记录警告
    return rc;                                                                     // 返回错误码
  }

  // 找到对应的 group
  for (GroupType &group : groups_) {                                       // 遍历所有分组
    int compare_result = 0;                                                // 比较结果初始化为 0
    rc = group_by_evaluated_tuple.compare(get<0>(group), compare_result);  // 比较分组值和当前元组的值
    if (OB_FAIL(rc)) {                                                     // 如果比较失败
      LOG_WARN("failed to compare group by values. rc=%s", strrc(rc));      // 记录警告
      return rc;                                                           // 返回错误码
    }

    if (compare_result == 0) {  // 如果找到对应的分组
      found_group = &group;     // 设置找到的分组
      break;                    // 跳出循环
    }
  }

  // 如果没有找到对应的 group，创建一个新的 group
  if (nullptr == found_group) {               // 如果没有找到分组
    AggregatorList aggregator_list;           // 聚合器列表
    create_aggregator_list(aggregator_list);  // 创建聚合器列表

    ValueListTuple child_tuple_to_value;                                // 存储当前子元组的值列表
    rc = ValueListTuple::make(child_tuple, child_tuple_to_value);       // 从子元组生成值列表
    if (OB_FAIL(rc)) {                                                  // 如果生成失败
      LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));  // 记录警告
      return rc;                                                        // 返回错误码
    }

    CompositeTuple composite_tuple;  // 创建复合元组
    composite_tuple.add_tuple(
        make_unique<ValueListTuple>(std::move(child_tuple_to_value)));  // 将当前子元组值列表添加到复合元组中
    groups_.emplace_back(std::move(group_by_evaluated_tuple),           // 创建新的分组并添加到 groups_ 中
        GroupValueType(std::move(aggregator_list), std::move(composite_tuple)));
    found_group = &groups_.back();  // 设置找到的分组为新添加的分组
  }

  return rc;  // 返回状态码
}
