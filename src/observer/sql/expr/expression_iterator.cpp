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
// Created by Wangyunlai on 2024/05/30.
//

#include "sql/expr/expression_iterator.h"
#include "sql/expr/expression.h"
#include "common/log/log.h"

using namespace std;

/**
 * @brief 遍历表达式树的子表达式
 *
 * 该函数接受一个表达式和一个回调函数，遍历该表达式的所有子表达式，并对每个子表达式调用回调函数。
 *
 * @param expr 要遍历的表达式。
 * @param callback 处理子表达式的回调函数。
 * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回相应的错误码。
 */
RC ExpressionIterator::iterate_child_expr(Expression &expr, function<RC(unique_ptr<Expression> &)> callback)
{
  RC rc = RC::SUCCESS;  // 初始化返回状态码为成功

  // 根据表达式类型处理不同的子表达式
  switch (expr.type()) {
    case ExprType::CAST: {  // 类型转换表达式
      auto &cast_expr = static_cast<CastExpr &>(expr);
      rc              = callback(cast_expr.child());  // 处理子表达式
    } break;

    case ExprType::COMPARISON: {  // 比较表达式
      auto &comparison_expr = static_cast<ComparisonExpr &>(expr);
      rc                    = callback(comparison_expr.left());  // 处理左子表达式

      if (OB_SUCC(rc)) {  // 如果处理成功，则继续处理右子表达式
        rc = callback(comparison_expr.right());
      }

    } break;

    case ExprType::CONJUNCTION: {  // 连接表达式（如 AND、OR）
      auto &conjunction_expr = static_cast<ConjunctionExpr &>(expr);
      // 遍历所有子表达式
      for (auto &child : conjunction_expr.children()) {
        rc = callback(child);  // 处理每个子表达式
        if (OB_FAIL(rc)) {     // 如果处理失败，则提前退出
          break;
        }
      }
    } break;

    case ExprType::ARITHMETIC: {  // 算术表达式
      auto &arithmetic_expr = static_cast<ArithmeticExpr &>(expr);
      rc                    = callback(arithmetic_expr.left());  // 处理左子表达式
      if (OB_SUCC(rc)) {                                         // 如果处理成功，则继续处理右子表达式
        rc = callback(arithmetic_expr.right());
      }
    } break;

    case ExprType::AGGREGATION: {  // 聚合表达式（如 SUM、AVG）
      auto &aggregate_expr = static_cast<AggregateExpr &>(expr);
      rc                   = callback(aggregate_expr.child());  // 处理子表达式
    } break;

    case ExprType::NONE:           // 空表达式
    case ExprType::STAR:           // 星号表达式（如 SELECT *）
    case ExprType::UNBOUND_FIELD:  // 未绑定字段
    case ExprType::FIELD:          // 字段表达式
    case ExprType::VALUE: {        // 值表达式
      // 不处理任何子表达式，直接返回
    } break;

    default: {                                  // 未知表达式类型
      ASSERT(false, "Unknown expression type");  // 触发断言，说明代码存在问题
    }
  }

  return rc;  // 返回操作结果
}
