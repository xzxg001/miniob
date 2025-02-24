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
// Created by WangYunlai on 2022/12/08.
//

#pragma once  // 防止头文件被多次包含

#include <memory>  // 引入智能指针支持
#include <vector>  // 引入动态数组支持

#include "sql/expr/expression.h"            // 引入表达式相关定义
#include "sql/operator/logical_operator.h"  // 引入逻辑算子相关定义
#include "storage/field/field.h"            // 引入字段相关定义

/**
 * @brief project 表示投影运算
 * @ingroup LogicalOperator
 * @details 从表中获取数据后，可能需要过滤，投影，连接等等。
 */
class ProjectLogicalOperator : public LogicalOperator  // 定义ProjectLogicalOperator类，继承自LogicalOperator
{
public:
  /**
   * @brief 构造函数，初始化投影逻辑算子
   * @param expressions 用于投影的表达式列表
   */
  ProjectLogicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions);  // 构造函数声明

  virtual ~ProjectLogicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取逻辑算子的类型
   * @return 返回逻辑算子的类型（PROJECTION）
   */
  LogicalOperatorType type() const override { return LogicalOperatorType::PROJECTION; }

  /**
   * @brief 获取表达式列表
   * @return 返回可修改的表达式列表
   */
  std::vector<std::unique_ptr<Expression>> &expressions() { return expressions_; }

  /**
   * @brief 获取表达式列表
   * @return 返回不可修改的表达式列表
   */
  const std::vector<std::unique_ptr<Expression>> &expressions() const { return expressions_; }

private:
  std::vector<std::unique_ptr<Expression>> expressions_;  // 表达式列表，用于存储投影表达式
};
