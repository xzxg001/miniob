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
// Created by Wangyunlai on 2022/12/29.
//
#include "sql/optimizer/rewriter.h"  // 包含重写器的头文件
#include "common/log/log.h"  // 包含日志记录的头文件
#include "sql/operator/logical_operator.h"  // 包含逻辑操作符的头文件
#include "sql/optimizer/expression_rewriter.h"  // 包含表达式重写器的头文件
#include "sql/optimizer/predicate_pushdown_rewriter.h"  // 包含谓词下推重写器的头文件
#include "sql/optimizer/predicate_rewrite.h"  // 包含谓词重写规则的头文件

// Rewriter类的构造函数
Rewriter::Rewriter()
{
  // 添加表达式重写规则
  rewrite_rules_.emplace_back(new ExpressionRewriter);
  // 添加谓词重写规则
  rewrite_rules_.emplace_back(new PredicateRewriteRule);
  // 添加谓词下推重写规则
  rewrite_rules_.emplace_back(new PredicatePushdownRewriter);
}

// rewrite函数用于重写逻辑操作符
RC Rewriter::rewrite(std::unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  change_made = false;  // 初始化变化标记为false
  // 遍历所有的重写规则
  for (std::unique_ptr<RewriteRule> &rule : rewrite_rules_) {
    bool sub_change_made = false;  // 初始化子变化标记为false

    // 应用规则重写逻辑操作符
    rc = rule->rewrite(oper, sub_change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to rewrite logical operator. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 如果重写失败，返回失败的返回码
    }

    // 如果子操作符发生了变化，且之前没有标记变化，则标记变化
    if (sub_change_made && !change_made) {
      change_made = true;
    }
  }

  if (rc != RC::SUCCESS) {
    return rc;  // 如果返回码不是成功，返回失败
  }

  // 获取逻辑操作符的子操作符列表
  std::vector<std::unique_ptr<LogicalOperator>> &child_opers = oper->children();
  // 遍历每个子操作符
  for (auto &child_oper : child_opers) {
    bool sub_change_made = false;  // 初始化子变化标记为false
    // 递归重写子操作符
    rc = this->rewrite(child_oper, sub_change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to rewrite child oper. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 如果重写失败，返回失败的返回码
    }

    // 如果子操作符发生了变化，且之前没有标记变化，则标记变化
    if (sub_change_made && !change_made) {
      change_made = true;
    }
  }
  return rc;  // 返回返回码
}
