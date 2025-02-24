/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include "common/log/log.h"
#include "sql/operator/aggregate_vec_physical_operator.h"
#include "sql/expr/aggregate_state.h"
#include "sql/expr/expression_tuple.h"
#include "sql/expr/composite_tuple.h"

using namespace std;
using namespace common;

/**
 * @brief 聚合向量物理操作符实现类。
 * @ingroup Operator
 */
AggregateVecPhysicalOperator::AggregateVecPhysicalOperator(vector<Expression *> &&expressions)
{
  // 将聚合表达式移动到成员变量中
  aggregate_expressions_ = std::move(expressions);
  value_expressions_.reserve(aggregate_expressions_.size());

  // 遍历聚合表达式，初始化子表达式并确保其有效性
  ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregation expression must have a child expression");
    value_expressions_.emplace_back(child_expr);
  });

  // 初始化聚合状态和输出列
  for (size_t i = 0; i < aggregate_expressions_.size(); i++) {
    auto &expr = aggregate_expressions_[i];
    ASSERT(expr->type() == ExprType::AGGREGATION, "expected an aggregation expression");
    auto *aggregate_expr = static_cast<AggregateExpr *>(expr);

    // 根据聚合类型初始化状态
    if (aggregate_expr->aggregate_type() == AggregateExpr::Type::SUM) {
      if (aggregate_expr->value_type() == AttrType::INTS) {
        void *aggr_value                     = malloc(sizeof(SumState<int>));
        ((SumState<int> *)aggr_value)->value = 0;
        aggr_values_.insert(aggr_value);
        output_chunk_.add_column(make_unique<Column>(AttrType::INTS, sizeof(int)), i);
      } else if (aggregate_expr->value_type() == AttrType::FLOATS) {
        void *aggr_value                       = malloc(sizeof(SumState<float>));
        ((SumState<float> *)aggr_value)->value = 0;
        aggr_values_.insert(aggr_value);
        output_chunk_.add_column(make_unique<Column>(AttrType::FLOATS, sizeof(float)), i);
      }
    } else {
      ASSERT(false, "not supported aggregation type");
    }
  }
}

/**
 * @brief 打开聚合向量物理操作符，准备执行。
 * @param trx 事务指针。
 * @return 返回状态码，成功为 `RC::SUCCESS`，失败时返回相应错误码。
 */
RC AggregateVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  // 处理每一行数据
  while (OB_SUCC(rc = child.next(chunk_))) {
    for (size_t aggr_idx = 0; aggr_idx < aggregate_expressions_.size(); aggr_idx++) {
      Column column;
      value_expressions_[aggr_idx]->get_column(chunk_, column);
      ASSERT(aggregate_expressions_[aggr_idx]->type() == ExprType::AGGREGATION, "expect aggregate expression");
      auto *aggregate_expr = static_cast<AggregateExpr *>(aggregate_expressions_[aggr_idx]);

      // 更新聚合状态
      if (aggregate_expr->aggregate_type() == AggregateExpr::Type::SUM) {
        if (aggregate_expr->value_type() == AttrType::INTS) {
          update_aggregate_state<SumState<int>, int>(aggr_values_.at(aggr_idx), column);
        } else if (aggregate_expr->value_type() == AttrType::FLOATS) {
          update_aggregate_state<SumState<float>, float>(aggr_values_.at(aggr_idx), column);
        } else {
          ASSERT(false, "not supported value type");
        }
      } else {
        ASSERT(false, "not supported aggregation type");
      }
    }
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;  // 处理结束时，设置返回值为成功
  }

  return rc;
}

/**
 * @brief 更新聚合状态。
 * @tparam STATE 聚合状态类型。
 * @tparam T 数据类型。
 * @param state 聚合状态的指针。
 * @param column 列对象，包含要聚合的数据。
 */
template <class STATE, typename T>
void AggregateVecPhysicalOperator::update_aggregate_state(void *state, const Column &column)
{
  STATE *state_ptr = reinterpret_cast<STATE *>(state);
  T     *data      = (T *)column.data();
  state_ptr->update(data, column.count());  // 更新聚合状态
}

/**
 * @brief 获取下一行数据并进行处理。
 * @param chunk 存储结果的块。
 * @return 返回状态码，成功为 `RC::SUCCESS`，失败时返回相应错误码。
 */
RC AggregateVecPhysicalOperator::next(Chunk &chunk)
{
  // 这里需要实现获取下一行数据的逻辑
  exit(-1);  // 暂时未实现
}

/**
 * @brief 关闭聚合向量物理操作符，释放资源。
 * @return 返回状态码，成功为 `RC::SUCCESS`。
 */
RC AggregateVecPhysicalOperator::close()
{
  children_[0]->close();  // 关闭子操作符
  LOG_INFO("close group by operator");
  return RC::SUCCESS;
}
