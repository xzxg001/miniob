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

#include "sql/operator/insert_logical_operator.h"  // 包含插入逻辑算子的头文件

// 构造函数实现
InsertLogicalOperator::InsertLogicalOperator(Table *table, std::vector<Value> values)
    : table_(table),   // 初始化表指针
      values_(values)  // 初始化值向量
{}
