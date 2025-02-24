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
// Created by WangYunlai on 2022/12/27.
//

#pragma once  // 确保该头文件仅被包含一次

#include "sql/operator/logical_operator.h"  // 引入逻辑算子的头文件

/**
 * @brief Explain逻辑算子
 *
 * 该类表示用于执行解释操作的逻辑算子。它继承自
 * LogicalOperator 基类，并实现了特定的逻辑算子类型。
 * @ingroup LogicalOperator
 */
class ExplainLogicalOperator : public LogicalOperator
{
public:
  ExplainLogicalOperator()          = default;  // 默认构造函数
  virtual ~ExplainLogicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取逻辑算子的类型
   *
   * @return LogicalOperatorType 返回当前逻辑算子的类型
   *
   * 此方法重载自基类，返回解释算子的逻辑类型。
   */
  LogicalOperatorType type() const override { return LogicalOperatorType::EXPLAIN; }  // 返回逻辑算子类型为 EXPLAIN
};
