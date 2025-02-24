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
// Created by Wangyunlai on 2022/12/07.
//

#pragma once

#include <vector>
#include "sql/operator/logical_operator.h"

/**
 * @brief 逻辑算子，用于描述当前执行计划的计算操作
 *
 * @details 该类表示在查询优化阶段生成的逻辑操作，它包含了一组表达式
 * 用于计算特定的值。在执行计划中，该算子会被转化为对应的物理算子。
 */
class CalcLogicalOperator : public LogicalOperator
{
public:
  /**
   * @brief 构造函数
   * @param expressions 包含的计算表达式集合，使用右值引用传递以支持移动语义
   */
  CalcLogicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions)
  {
    expressions_.swap(expressions);  // 将传入的表达式移动到内部成员
  }

  virtual ~CalcLogicalOperator() = default;  // 默认析构函数

  /**
   * @brief 返回逻辑算子的类型
   * @return 逻辑算子类型，这里返回CALC类型
   */
  LogicalOperatorType type() const override { return LogicalOperatorType::CALC; }

private:
  std::vector<std::unique_ptr<Expression>> expressions_;  // 存储用于计算的表达式集合
};
