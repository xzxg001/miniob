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
// Created by WangYunlai on 2023/4/25.
// 文件创建时间：2023年4月25日
//

#pragma once

#include <vector>  // 包含标准向量库

#include "sql/operator/logical_operator.h"  // 包含逻辑算子基类
#include "sql/parser/parse_defs.h"          // 包含解析定义

/**
 * @brief 插入逻辑算子
 * @ingroup LogicalOperator
 */
class InsertLogicalOperator : public LogicalOperator
{
public:
  // 构造函数
  InsertLogicalOperator(Table *table, std::vector<Value> values);
  virtual ~InsertLogicalOperator() = default;  // 默认析构函数

  // 返回逻辑算子类型
  LogicalOperatorType type() const override { return LogicalOperatorType::INSERT; }

  // 获取表指针
  Table *table() const { return table_; }
  // 获取值的常量引用
  const std::vector<Value> &values() const { return values_; }
  // 获取值的可变引用
  std::vector<Value> &values() { return values_; }

private:
  Table             *table_ = nullptr;  // 表指针
  std::vector<Value> values_;           // 存储插入值的向量
};
