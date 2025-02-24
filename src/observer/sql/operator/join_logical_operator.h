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
// Created by Wangyunlai on 2022/12/07
// 文件创建时间：2022年12月7日
//

#pragma once

#include "sql/operator/logical_operator.h"  // 包含逻辑算子的基类头文件

/**
 * @brief 连接算子
 * @ingroup LogicalOperator
 * @details 连接算子，用于连接两个表。对应的物理算子或者实现，可能有 NestedLoopJoin，HashJoin 等等。
 */
class JoinLogicalOperator : public LogicalOperator
{
public:
  JoinLogicalOperator()          = default;  // 默认构造函数
  virtual ~JoinLogicalOperator() = default;  // 默认析构函数

  // 返回算子的类型，这里返回的是 JOIN 类型
  LogicalOperatorType type() const override { return LogicalOperatorType::JOIN; }

private:
  // 这里可以添加连接算子的特定成员变量，例如连接条件、左表、右表等
};
