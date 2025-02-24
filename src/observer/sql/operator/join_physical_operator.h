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
// Created by WangYunlai on 2021/6/10.
// 文件创建时间：2021年6月10日
//

#pragma once

#include "sql/operator/physical_operator.h"  // 包含物理算子的基类头文件
#include "sql/parser/parse.h"                // 包含解析器的相关定义

/**
 * @brief 最简单的两表（称为左表、右表）join算子
 * @details 依次遍历左表的每一行，然后关联右表的每一行
 * @ingroup PhysicalOperator
 */
class NestedLoopJoinPhysicalOperator : public PhysicalOperator
{
public:
  NestedLoopJoinPhysicalOperator();                     // 构造函数
  virtual ~NestedLoopJoinPhysicalOperator() = default;  // 默认析构函数

  // 返回算子的类型，这里返回的是 NESTED_LOOP_JOIN 类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::NESTED_LOOP_JOIN; }

  // 打开算子，初始化相关资源
  RC open(Trx *trx) override;

  // 获取下一个结果元组
  RC next() override;

  // 关闭算子，释放相关资源
  RC close() override;

  // 获取当前关联的元组
  Tuple *current_tuple() override;

private:
  // 左表遍历下一条数据
  RC left_next();

  // 右表遍历下一条数据，如果上一轮结束了就重新开始新的一轮
  RC right_next();

private:
  Trx *trx_ = nullptr;  // 当前事务

  // 左表和右表的真实对象是在 PhysicalOperator::children_ 中，这里是为了写的时候更简单
  PhysicalOperator *left_        = nullptr;  // 左表物理算子
  PhysicalOperator *right_       = nullptr;  // 右表物理算子
  Tuple            *left_tuple_  = nullptr;  // 当前左表元组
  Tuple            *right_tuple_ = nullptr;  // 当前右表元组
  JoinedTuple       joined_tuple_;           // 当前关联的左右两个元组
  bool              round_done_   = true;    // 右表遍历的一轮是否结束
  bool              right_closed_ = true;    // 右表算子是否已经关闭
};
