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
// Created by Wangyunlai on 2022/12/15
//

#include "sql/operator/table_get_logical_operator.h"  // 引入表获取逻辑算子的头文件

// 构造函数，接受表对象和读写模式
TableGetLogicalOperator::TableGetLogicalOperator(Table *table, ReadWriteMode mode)
    : table_(table), mode_(mode)  // 初始化成员变量
{}

// 设置过滤条件
void TableGetLogicalOperator::set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);  // 移动传入的表达式，避免不必要的复制
}
