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

#include "sql/expr/tuple.h"

/**
 * @brief 组合的Tuple
 *
 * CompositeTuple类用于表示多个元组的组合，支持对组合元组的单元格数、单元格值和规格的访问。
 * @ingroup Tuple
 * TODO 单元测试
 */
class CompositeTuple : public Tuple
{
public:
  CompositeTuple()          = default;  ///< 默认构造函数
  virtual ~CompositeTuple() = default;  ///< 默认析构函数

  /// @brief 删除默认复制构造函数
  CompositeTuple(const CompositeTuple &) = delete;

  /// @brief 删除默认赋值操作符
  CompositeTuple &operator=(const CompositeTuple &) = delete;

  /// @brief 保留移动构造函数
  CompositeTuple(CompositeTuple &&) = default;

  /// @brief 保留移动赋值操作符
  CompositeTuple &operator=(CompositeTuple &&) = default;

  /**
   * @brief 计算当前复合元组中所有元组的单元格总数。
   * @return int 返回复合元组中单元格的数量。
   */
  int cell_num() const override;

  /**
   * @brief 获取指定索引位置的单元格的值。
   * @param index 要访问的单元格索引。
   * @param cell 存放单元格值的引用。
   * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
   */
  RC cell_at(int index, Value &cell) const override;

  /**
   * @brief 获取指定索引位置的单元格规格。
   * @param index 要访问的单元格索引。
   * @param spec 存放单元格规格的引用。
   * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
   */
  RC spec_at(int index, TupleCellSpec &spec) const override;

  /**
   * @brief 根据单元格规格查找相应的单元格值。
   * @param spec 要查找的单元格规格。
   * @param cell 存放找到的单元格值的引用。
   * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
   */
  RC find_cell(const TupleCellSpec &spec, Value &cell) const override;

  /**
   * @brief 添加一个子元组到复合元组中。
   * @param tuple 要添加的子元组的唯一指针。
   */
  void add_tuple(std::unique_ptr<Tuple> tuple);

  /**
   * @brief 获取指定索引位置的子元组。
   * @param index 要访问的子元组索引。
   * @return Tuple& 返回指定索引位置的子元组的引用。
   */
  Tuple &tuple_at(size_t index);

private:
  std::vector<std::unique_ptr<Tuple>> tuples_;  ///< 存储组合元组的子元组
};
