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
// Created by WangYunlai on 2022/07/01.
//

#pragma once

#include "sql/expr/expression_tuple.h"
#include "sql/operator/physical_operator.h"

/**
 * @brief 计算物理算子
 *
 * @details 此类负责执行计算操作。它利用表达式计算得到的值，并将这些值
 * 存储在一个元组中以供后续使用。该算子主要用于执行计算表达式的逻辑。
 */
class CalcPhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief 构造函数
   * @param expressions 包含的计算表达式集合，使用右值引用传递以支持移动语义
   */
  CalcPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions)
      : expressions_(std::move(expressions)), tuple_(expressions_)  // 初始化表达式和元组
  {}

  virtual ~CalcPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 返回物理算子的类型
   * @return 物理算子类型，标识为CALC
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::CALC; }

  /**
   * @brief 返回物理算子的名称
   * @return 算子的名称，"CALC"
   */
  std::string name() const override { return "CALC"; }

  /**
   * @brief 返回物理算子的参数（目前为空）
   * @return 空字符串
   */
  std::string param() const override { return ""; }

  /**
   * @brief 打开算子，准备进行计算
   * @param trx 当前事务
   * @return RC::SUCCESS 表示成功
   */
  RC open(Trx *trx) override { return RC::SUCCESS; }

  /**
   * @brief 计算下一个元组的值
   * @return RC::SUCCESS 表示成功，RC::RECORD_EOF 表示没有更多记录
   */
  RC next() override
  {
    RC rc = RC::SUCCESS;
    if (emitted_) {  // 如果已经发出过记录，则返回记录结束标志
      rc = RC::RECORD_EOF;
      return rc;
    }
    emitted_ = true;  // 设置为已发出状态

    int cell_num = tuple_.cell_num();  // 获取元组的单元格数量
    for (int i = 0; i < cell_num; i++) {
      Value value;
      rc = tuple_.cell_at(i, value);  // 获取元组中的单元格值
      if (OB_FAIL(rc)) {
        return rc;  // 如果获取失败，返回错误码
      }
    }
    return RC::SUCCESS;  // 返回成功
  }

  /**
   * @brief 关闭算子，释放资源
   * @return RC::SUCCESS 表示成功
   */
  RC close() override { return RC::SUCCESS; }

  /**
   * @brief 获取当前元组的单元格数量
   * @return 单元格数量
   */
  int cell_num() const { return tuple_.cell_num(); }

  /**
   * @brief 获取当前元组的指针
   * @return 当前元组的指针
   */
  Tuple *current_tuple() override { return &tuple_; }

  /**
   * @brief 获取表达式集合的引用
   * @return 表达式集合的引用
   */
  const std::vector<std::unique_ptr<Expression>> &expressions() const { return expressions_; }

  /**
   * @brief 获取元组的模式（schema）
   * @param schema 用于存储模式的对象
   * @return RC::SUCCESS 表示成功
   */
  RC tuple_schema(TupleSchema &schema) const override
  {
    for (const std::unique_ptr<Expression> &expression : expressions_) {
      schema.append_cell(expression->name());  // 将表达式的名称添加到模式中
    }
    return RC::SUCCESS;  // 返回成功
  }

private:
  std::vector<std::unique_ptr<Expression>>     expressions_;      // 存储的计算表达式集合
  ExpressionTuple<std::unique_ptr<Expression>> tuple_;            // 存储计算结果的元组
  bool                                         emitted_ = false;  // 是否已发出记录的标志
};
