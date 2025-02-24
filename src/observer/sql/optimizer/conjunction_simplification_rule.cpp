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
// Created by Wangyunlai on 2022/12/26.
//

#include "sql/optimizer/conjunction_simplification_rule.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"

RC try_to_get_bool_constant(std::unique_ptr<Expression> &expr, bool &constant_value)
{
  // 如果表达式类型为值表达式，并且值类型为布尔型
  if (expr->type() == ExprType::VALUE && expr->value_type() == AttrType::BOOLEANS) {
    // 将表达式转换为值表达式类型
    auto value_expr = static_cast<ValueExpr *>(expr.get());
    // 获取布尔值
    constant_value  = value_expr->get_value().get_boolean();
    return RC::SUCCESS;  // 返回成功状态码
  }
  return RC::INTERNAL;  // 如果表达式不是布尔值类型，返回内部错误状态码
}
RC ConjunctionSimplificationRule::rewrite(std::unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 如果表达式类型不是逻辑合取或析取，直接返回
  if (expr->type() != ExprType::CONJUNCTION) {
    return rc;
  }

  change_made = false;  // 初始化标志位，表示没有进行任何修改

  // 将表达式转换为逻辑合取或析取表达式类型
  auto conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

  // 获取子表达式列表的引用
  std::vector<std::unique_ptr<Expression>> &child_exprs = conjunction_expr->children();

  // 遍历子表达式列表，尝试简化
  for (auto iter = child_exprs.begin(); iter != child_exprs.end();) {
    bool constant_value = false;  // 初始化布尔常量值

    // 尝试获取布尔常量值
    rc = try_to_get_bool_constant(*iter, constant_value);
    // 如果获取失败，跳过当前表达式，继续下一个
    if (rc != RC::SUCCESS) {
      rc = RC::SUCCESS;
      ++iter;
      continue;
    }

    // 如果是逻辑合取（AND）
    if (conjunction_expr->conjunction_type() == ConjunctionExpr::Type::AND) {
      // 如果常量值为true，可以删除该表达式
      if (constant_value == true) {
        child_exprs.erase(iter);  // 删除表达式
      } else {
        // 如果常量值为false，整个表达式恒为false
        std::unique_ptr<Expression> child_expr = std::move(child_exprs.front());
        child_exprs.clear();  // 清空子表达式列表
        expr = std::move(child_expr);  // 更新表达式
        return rc;  // 返回
      }
    } else {
      // 如果是逻辑析取（OR）
      if (constant_value == true) {
        // 如果常量值为true，整个表达式恒为true
        std::unique_ptr<Expression> child_expr = std::move(child_exprs.front());
        child_exprs.clear();  // 清空子表达式列表
        expr = std::move(child_expr);  // 更新表达式
        return rc;  // 返回
      } else {
        child_exprs.erase(iter);  // 删除表达式
      }
    }
  }

  // 如果子表达式列表中只剩下一个表达式，直接返回该表达式
  if (child_exprs.size() == 1) {
    LOG_TRACE("conjunction expression has only 1 child");  // 记录日志
    std::unique_ptr<Expression> child_expr = std::move(child_exprs.front());
    child_exprs.clear();  // 清空子表达式列表
    expr = std::move(child_expr);  // 更新表达式
    change_made = true;  // 标记为修改了表达式
  }

  return rc;  // 返回操作结果
}