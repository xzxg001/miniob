/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief 聚合物理算子 (Vectorized)
 * @ingroup PhysicalOperator
 * 该类实现了聚合操作的物理算子，支持向量化计算以提高性能。
 */
class AggregateVecPhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief 构造函数
   * @param expressions 聚合表达式的集合
   */
  AggregateVecPhysicalOperator(std::vector<Expression *> &&expressions);

  virtual ~AggregateVecPhysicalOperator() = default;

  /**
   * @brief 返回物理算子的类型
   * @return 物理算子类型
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::AGGREGATE_VEC; }

  /**
   * @brief 打开聚合物理算子，准备执行
   * @param trx 事务指针
   * @return 返回状态码，成功为 `RC::SUCCESS`，失败时返回相应错误码
   */
  RC open(Trx *trx) override;

  /**
   * @brief 获取下一组结果
   * @param chunk 存储结果的块
   * @return 返回状态码，成功为 `RC::SUCCESS`，失败时返回相应错误码
   */
  RC next(Chunk &chunk) override;

  /**
   * @brief 关闭聚合物理算子，释放资源
   * @return 返回状态码，成功为 `RC::SUCCESS`
   */
  RC close() override;

private:
  /**
   * @brief 更新聚合状态
   * @tparam STATE 聚合状态类型
   * @tparam T 数据类型
   * @param state 聚合状态指针
   * @param column 包含数据的列
   */
  template <class STATE, typename T>
  void update_aggregate_state(void *state, const Column &column);

  /**
   * @brief 将聚合结果追加到输出列
   * @tparam STATE 聚合状态类型
   * @tparam T 数据类型
   * @param state 聚合状态指针
   * @param column 输出列
   */
  template <class STATE, typename T>
  void append_to_column(void *state, Column &column)
  {
    STATE *state_ptr = reinterpret_cast<STATE *>(state);
    column.append_one((char *)&state_ptr->value);  // 将聚合值添加到列中
  }

private:
  /**
   * @brief 聚合值管理类
   * 用于存储和管理聚合状态的内存
   */
  class AggregateValues
  {
  public:
    AggregateValues() = default;

    /**
     * @brief 插入聚合值
     * @param aggr_value 聚合值的指针
     */
    void insert(void *aggr_value) { data_.push_back(aggr_value); }

    /**
     * @brief 获取指定索引的聚合值
     * @param index 聚合值的索引
     * @return 返回聚合值的指针
     */
    void *at(size_t index)
    {
      ASSERT(index < data_.size(), "index out of range");  // 确保索引有效
      return data_[index];
    }

    /**
     * @brief 获取聚合值的数量
     * @return 聚合值的数量
     */
    size_t size() { return data_.size(); }

    /**
     * @brief 析构函数，释放聚合值的内存
     */
    ~AggregateValues()
    {
      for (auto &aggr_value : data_) {
        free(aggr_value);  // 释放聚合值的内存
        aggr_value = nullptr;
      }
    }

  private:
    std::vector<void *> data_;  // 存储聚合值的指针
  };

  std::vector<Expression *> aggregate_expressions_;  // 聚合表达式的集合
  std::vector<Expression *> value_expressions_;      // 子表达式的集合
  Chunk                     chunk_;                  // 当前处理的数据块
  Chunk                     output_chunk_;           // 输出结果的数据块
  AggregateValues           aggr_values_;            // 聚合值管理
};
