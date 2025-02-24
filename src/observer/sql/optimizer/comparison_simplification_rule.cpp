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
// Created by Wangyunlai on 2022/12/13.
//
#include "sql/optimizer/comparison_simplification_rule.h"  // 包含比较简化规则类的声明
#include "common/log/log.h"  // 包含日志记录功能
#include "sql/expr/expression.h"  // 包含SQL表达式相关的声明

// 重写表达式的成员函数，尝试简化比较表达式
RC ComparisonSimplificationRule::rewrite(std::unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  change_made = false;  // 初始化标志位，表示没有进行任何修改

  // 如果表达式类型为比较表达式
  if (expr->type() == ExprType::COMPARISON) {
    Value value;  // 创建一个值对象，用于存储从比较表达式中获取的值

    ComparisonExpr *cmp_expr = static_cast<ComparisonExpr *>(expr.get());  // 将表达式转换为比较表达式类型

    // 尝试从比较表达式中获取一个值
    RC sub_rc = cmp_expr->try_get_value(value);
    if (sub_rc == RC::SUCCESS) {
      // 如果成功获取值，创建一个新的值表达式
      std::unique_ptr<Expression> new_expr(new ValueExpr(value));
      // 将原始表达式替换为新的值表达式
      expr.swap(new_expr);
      change_made = true;  // 标记为修改了表达式
      LOG_TRACE("comparison expression is simplified");  // 记录日志，表示比较表达式已被简化
    }
  }
  return rc;  // 返回操作结果，通常是成功状态码
}