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
// Created by WangYunlai on 2022/12/26.
//

#include "sql/operator/delete_logical_operator.h"  // 包含 DeleteLogicalOperator 的头文件

/**
 * @brief 删除逻辑算子的构造函数
 *
 * @param table 指向要删除的表的指针
 *
 * @details 构造函数接受一个表的指针，并将其赋值给成员变量 table_。
 * 这个指针用于在后续的操作中指向具体的删除目标表，以便执行删除逻辑。
 */
DeleteLogicalOperator::DeleteLogicalOperator(Table *table)
    : table_(table)  // 使用成员初始化列表将传入的 table 指针赋值给私有成员变量 table_
{}
