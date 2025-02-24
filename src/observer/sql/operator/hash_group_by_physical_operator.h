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

#pragma once  // 防止重复包含

#include "sql/operator/group_by_physical_operator.h"  // 引入 GroupByPhysicalOperator 的头文件
#include "sql/expr/composite_tuple.h"                 // 引入复合元组的头文件

/**
 * @brief HashGroupByPhysicalOperator 类实现了基于哈希的分组聚合操作
 * @ingroup PhysicalOperator
 * @details 此类通过哈希的方式进行 group by 操作。当聚合函数存在 group by 表达式时，
 * 默认采用这个物理算子（当前也只有这个物理算子）。
 * NOTE: 当前并没有使用哈希方式实现，而是每次使用线性查找的方式。
 */
class HashGroupByPhysicalOperator : public GroupByPhysicalOperator
{
public:
  // 构造函数，接受两个参数，分别是分组表达式和聚合表达式
  HashGroupByPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&group_by_exprs,  // 分组表达式的唯一指针向量
      std::vector<Expression *> &&expressions);  // 聚合表达式的指针向量

  // 析构函数，默认实现
  virtual ~HashGroupByPhysicalOperator() = default;

  // 重写基类的 type 函数，返回操作符类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_GROUP_BY; }

  // 打开操作符，初始化状态，接受事务指针
  RC open(Trx *trx) override;

  // 获取下一行结果
  RC next() override;

  // 关闭操作符，释放资源
  RC close() override;

  // 获取当前的元组
  Tuple *current_tuple() override;

private:
  using AggregatorList = GroupByPhysicalOperator::AggregatorList;  // 别名，表示聚合函数列表
  using GroupValueType = GroupByPhysicalOperator::GroupValueType;  // 别名，表示组值类型
  /// 聚合出来的一组数据
  using GroupType = std::tuple<ValueListTuple, GroupValueType>;  // 定义一个元组，包含值列表和聚合值

private:
  // 查找当前行所属的分组
  RC find_group(const Tuple &child_tuple, GroupType *&found_group);  // 查找函数，接受子元组和找到的组

private:
  std::vector<std::unique_ptr<Expression>> group_by_exprs_;  // 存储分组表达式的唯一指针向量

  /// 一组一条数据
  /// pair的first是group by 的值列表，second是计算出来的表达式值列表
  /// TODO 改成hash/unordered_map
  std::vector<GroupType> groups_;  // 存储分组结果的向量，包含值列表和聚合值的元组

  std::vector<GroupType>::iterator current_group_;         // 当前分组的迭代器
  bool                             first_emited_ = false;  /// 第一条数据是否已经输出
};
