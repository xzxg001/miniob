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
#pragma once  // 确保头文件内容只被包含一次，防止重复包含

#include "common/rc.h"  // 包含通用的错误码定义
#include "sql/optimizer/rewrite_rule.h"  // 包含重写规则基类的声明

class LogicalOperator;  // 声明逻辑操作符类，可能在比较简化规则中使用

/**
 * @brief 简单比较的重写规则
 * @ingroup Rewriter
 * @details 如果有简单的比较运算，比如比较的两边都是常量，那我们就可以在运行执行计划之前就知道结果，
 * 进而直接将表达式改成结果，这样就可以减少运行时的计算量。
 */
class ComparisonSimplificationRule : public ExpressionRewriteRule  // 继承自表达式重写规则基类
{
public:
  ComparisonSimplificationRule() = default;  // 默认构造函数
  virtual ~ComparisonSimplificationRule() = default;  // 默认虚析构函数

  // 重写基类的rewrite函数，用于简化比较表达式
  RC rewrite(std::unique_ptr<Expression> &expr, bool &change_made) override;

private:
};
