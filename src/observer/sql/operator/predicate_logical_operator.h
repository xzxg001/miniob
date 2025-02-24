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

#pragma once  // 防止头文件重复包含

#include "sql/expr/expression.h"            // 引入表达式相关定义
#include "sql/operator/logical_operator.h"  // 引入逻辑算子定义

/**
 * @brief 谓词/过滤逻辑算子
 * @ingroup LogicalOperator
 * @details 该类表示逻辑执行计划中的过滤操作（谓词），用于在执行计划中应用条件。
 */
class PredicateLogicalOperator : public LogicalOperator  // 继承自LogicalOperator类
{
public:
  /**
   * @brief 构造函数，初始化PredicateLogicalOperator
   * @param expression 传入的表达式，表示过滤条件
   */
  PredicateLogicalOperator(std::unique_ptr<Expression> expression);

  // 默认析构函数
  virtual ~PredicateLogicalOperator() = default;

  /**
   * @brief 获取逻辑算子的类型
   * @return 返回逻辑算子的类型，具体为PREDICATE
   */
  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::PREDICATE;  // 返回PREDICATE类型
  }
};
