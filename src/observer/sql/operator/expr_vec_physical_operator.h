/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once  // 确保该头文件仅被编译器包含一次

#include "sql/operator/physical_operator.h"  // 引入物理算子的头文件

/**
 * @brief 表达式物理算子(Vectorized)
 *
 * 该类表示用于执行表达式的物理算子，支持向量化处理以提高性能。
 * 继承自 PhysicalOperator 基类，适用于处理多个表达式。
 * @ingroup PhysicalOperator
 */
class ExprVecPhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief 构造函数
   *
   * @param expressions 要处理的表达式列表
   *
   * 使用右值引用接收表达式列表，并将其存储在类的成员变量中。
   */
  ExprVecPhysicalOperator(std::vector<Expression *> &&expressions)
      : expressions_(std::move(expressions)) {}  // 初始化成员变量 expressions_

  virtual ~ExprVecPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取物理算子的类型
   *
   * @return PhysicalOperatorType 返回当前物理算子的类型
   *
   * 此方法重载自基类，返回表达式向量化算子的物理类型。
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::EXPR_VEC; }  // 返回物理算子类型为 EXPR_VEC

  RC open(Trx *trx) override;      // 打开算子，准备执行
  RC next(Chunk &chunk) override;  // 获取下一个结果并填充到 Chunk 中
  RC close() override;             // 关闭算子，释放资源

private:
  std::vector<Expression *> expressions_;   /// 表达式列表，存储要执行的表达式
  Chunk                     chunk_;         /// 当前处理的 Chunk
  Chunk                     evaled_chunk_;  /// 存储计算后的结果 Chunk
};
