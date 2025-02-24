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
#include "sql/optimizer/expression_rewriter.h"  // 包含表达式重写器的头文件
#include "common/log/log.h"  // 包含日志记录的头文件
#include "sql/optimizer/comparison_simplification_rule.h"  // 包含比较简化规则的头文件
#include "sql/optimizer/conjunction_simplification_rule.h"  // 包含合取简化规则的头文件

using namespace std;  // 使用标准命名空间

// ExpressionRewriter类的构造函数
ExpressionRewriter::ExpressionRewriter()
{
  expr_rewrite_rules_.emplace_back(new ComparisonSimplificationRule);  // 添加比较简化规则
  expr_rewrite_rules_.emplace_back(new ConjunctionSimplificationRule);  // 添加合取简化规则
}

// 重写操作的函数，接受一个逻辑操作符的指针和一个布尔值引用来记录是否发生了变化
RC ExpressionRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;  // 返回码初始化为成功

  bool sub_change_made = false;  // 子表达式变化的标记

  // 获取操作符的表达式列表
  vector<unique_ptr<Expression>> &expressions = oper->expressions();
  // 遍历每个表达式并尝试重写
  for (unique_ptr<Expression> &expr : expressions) {
    rc = rewrite_expression(expr, sub_change_made);  // 重写单个表达式
    if (rc != RC::SUCCESS) {  // 如果重写失败，退出循环
      break;
    }

    if (sub_change_made && !change_made) {  // 如果子表达式发生了变化，且之前没有标记变化，则标记变化
      change_made = true;
    }
  }

  if (rc != RC::SUCCESS) {  // 如果重写过程中返回码不是成功，则直接返回
    return rc;
  }

  // 获取操作符的子操作符列表
  vector<unique_ptr<LogicalOperator>> &child_opers = oper->children();
  // 遍历每个子操作符并尝试重写
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {
    bool sub_change_made = false;  // 子操作符变化的标记
    rc                   = rewrite(child_oper, sub_change_made);  // 递归重写子操作符
    if (sub_change_made && !change_made) {  // 如果子操作符发生了变化，且之前没有标记变化，则标记变化
      change_made = true;
    }
    if (rc != RC::SUCCESS) {  // 如果重写失败，退出循环
      break;
    }
  }
  return rc;  // 返回最终的返回码
}

// 重写单个表达式的函数
RC ExpressionRewriter::rewrite_expression(unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;  // 返回码初始化为成功

  change_made = false;  // 标记是否发生了变化，初始为false
  // 遍历所有的重写规则
  for (unique_ptr<ExpressionRewriteRule> &rule : expr_rewrite_rules_) {
    bool sub_change_made = false;  // 子表达式变化的标记

    rc                   = rule->rewrite(expr, sub_change_made);  // 应用规则重写表达式
    if (sub_change_made && !change_made) {  // 如果子表达式发生了变化，且之前没有标记变化，则标记变化
      change_made = true;
    }
    if (rc != RC::SUCCESS) {  // 如果重写失败，退出循环
      break;
    }
  }

  if (change_made || rc != RC::SUCCESS) {  // 如果发生了变化或者重写失败，则直接返回
    return rc;
  }

  // 根据表达式的类型进行不同的处理
  switch (expr->type()) {
    case ExprType::FIELD:  // 字段类型，不需要处理
    case ExprType::VALUE: {  // 值类型，不需要处理
      // do nothing
    } break;

    case ExprType::CAST: {  // 强制类型转换
      unique_ptr<Expression> &child_expr = (static_cast<CastExpr *>(expr.get()))->child();  // 获取子表达式

      rc                                      = rewrite_expression(child_expr, change_made);  // 递归重写子表达式
    } break;

    case ExprType::COMPARISON: {  // 比较表达式
      auto                         comparison_expr = static_cast<ComparisonExpr *>(expr.get());  // 转换为比较表达式类型

      unique_ptr<Expression> &left_expr       = comparison_expr->left();  // 获取左表达式
      unique_ptr<Expression> &right_expr      = comparison_expr->right();  // 获取右表达式

      bool left_change_made = false;  // 左表达式变化的标记

      rc                    = rewrite_expression(left_expr, left_change_made);  // 递归重写左表达式
      if (rc != RC::SUCCESS) {  // 如果重写失败，返回
        return rc;
      }

      bool right_change_made = false;  // 右表达式变化的标记

      rc                     = rewrite_expression(right_expr, right_change_made);  // 递归重写右表达式
      if (rc != RC::SUCCESS) {  // 如果重写失败，返回
        return rc;
      }

      if (left_change_made || right_change_made) {  // 如果左右表达式任一发生了变化，则标记变化
        change_made = true;
      }
    } break;

    case ExprType::CONJUNCTION: {  // 合取表达式
      auto                                      conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());  // 转换为合取表达式类型

      vector<unique_ptr<Expression>> &children         = conjunction_expr->children();  // 获取子表达式列表
      for (unique_ptr<Expression> &child_expr : children) {  // 遍历每个子表达式
        bool sub_change_made = false;  // 子表达式变化的标记

        rc                   = rewrite_expression(child_expr, sub_change_made);  // 递归重写子表达式
        if (rc != RC::SUCCESS) {  // 如果重写失败，记录日志并返回
          LOG_WARN("failed to rewriter conjunction sub expression. rc=%s", strrc(rc));  // 记录警告日志
          return rc;
        }

        if (sub_change_made && !change_made) {  // 如果子表达式发生了变化，且之前没有标记变化，则标记变化
          change_made = true;
        }
      }
    } break;

    default: {  // 其他类型的表达式，不需要处理
      // do nothing
    } break;
  }
  return rc;  // 返回最终的返回码
}