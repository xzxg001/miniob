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
// Created by Wangyunlai on 2024/5/31.
//

#pragma once

#include <vector>            // 引入 STL 的 vector 容器
#include "sql/expr/tuple.h"  // 引入 Tuple 类
#include "common/value.h"    // 引入 Value 类
#include "common/rc.h"       // 引入返回码定义

/**
 * @brief 表达式元组类
 *
 * 该类继承自 Tuple，表示一个由表达式构成的元组，支持通过表达式获取单元格的值。
 *
 * @tparam ExprPointerType 表达式指针的类型，通常为智能指针类型
 */
template <typename ExprPointerType>
class ExpressionTuple : public Tuple
{
public:
  /**
   * @brief 构造函数
   *
   * @param expressions 表达式指针的向量
   */
  ExpressionTuple(const std::vector<ExprPointerType> &expressions) : expressions_(expressions) {}
  virtual ~ExpressionTuple() = default;  // 默认析构函数

  /**
   * @brief 设置子元组
   *
   * @param tuple 子元组
   */
  void set_tuple(const Tuple *tuple) { child_tuple_ = tuple; }

  /**
   * @brief 获取单元格数量
   *
   * @return int 单元格的数量
   */
  int cell_num() const override { return static_cast<int>(expressions_.size()); }

  /**
   * @brief 获取指定索引的单元格的值
   *
   * @param index 单元格索引
   * @param cell 存储获取值的变量
   * @return RC 操作的状态码
   */
  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;  // 如果索引无效，返回错误码
    }

    const ExprPointerType &expression = expressions_[index];
    return get_value(expression, cell);  // 获取表达式对应的值
  }

  /**
   * @brief 获取指定索引的单元格的规格
   *
   * @param index 单元格索引
   * @param spec 存储单元格规格的变量
   * @return RC 操作的状态码
   */
  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;  // 如果索引无效，返回错误码
    }

    const ExprPointerType &expression = expressions_[index];
    spec = TupleCellSpec(expression->name());  // 设置单元格规格为表达式的名称
    return RC::SUCCESS;
  }

  /**
   * @brief 查找指定规格的单元格
   *
   * @param spec 要查找的单元格规格
   * @param cell 存储查找结果的变量
   * @return RC 操作的状态码
   */
  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = child_tuple_->find_cell(spec, cell);  // 先在子元组中查找
      if (OB_SUCC(rc)) {
        return rc;
      }
    }

    rc = RC::NOTFOUND;  // 默认未找到
    for (const ExprPointerType &expression : expressions_) {
      if (0 == strcmp(spec.alias(), expression->name())) {  // 匹配规格的名称
        rc = get_value(expression, cell);                   // 获取对应表达式的值
        break;
      }
    }

    return rc;
  }

private:
  /**
   * @brief 获取表达式对应的值
   *
   * @param expression 表达式指针
   * @param value 存储结果的变量
   * @return RC 操作的状态码
   */
  RC get_value(const ExprPointerType &expression, Value &value) const
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = expression->get_value(*child_tuple_, value);  // 从子元组获取值
    } else {
      rc = expression->try_get_value(value);  // 尝试直接获取值
    }
    return rc;
  }

private:
  const std::vector<ExprPointerType> &expressions_;            // 表达式指针的向量
  const Tuple                        *child_tuple_ = nullptr;  // 指向子元组的指针
};
