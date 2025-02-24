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
#pragma once  // 预处理指令，确保头文件只被包含一次

#include <memory>  // 包含C++标准库中的智能指针支持

#include "common/rc.h"  // 包含自定义的返回码定义
#include "sql/expr/expression.h"  // 包含SQL表达式的头文件
#include "sql/operator/logical_operator.h"  // 包含SQL逻辑操作符的头文件
#include "sql/optimizer/rewrite_rule.h"  // 包含重写规则基类的头文件

// ExpressionRewriter类继承自RewriteRule基类
class ExpressionRewriter : public RewriteRule
{
public:
  // 构造函数
  ExpressionRewriter();
  // 虚析构函数，允许派生类的析构函数被正确调用
  virtual ~ExpressionRewriter() = default;

  // 重写操作的函数，覆盖基类的虚函数
  RC rewrite(std::unique_ptr<LogicalOperator> &oper, bool &change_made) override;

private:
  // 重写单个表达式的私有成员函数
  RC rewrite_expression(std::unique_ptr<Expression> &expr, bool &change_made);

private:
  // 存储表达式重写规则的向量
  std::vector<std::unique_ptr<ExpressionRewriteRule>> expr_rewrite_rules_;
};