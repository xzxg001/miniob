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
// Created by Wangyunlai on 2022/5/22.
//
#include "sql/stmt/update_stmt.h"  // 包含更新语句类的头文件

// 构造函数，初始化更新语句所需的成员变量
UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

// 静态方法，用于创建UpdateStmt对象
RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // TODO: 这里应该包含根据提供的更新SQL节点（update）来创建更新语句对象的逻辑
  // 目前这部分代码还未实现，只是一个占位符
  
  // 初始化stmt为nullptr，表示创建失败或尚未创建对象
  stmt = nullptr;
  
  // 返回内部错误状态码，表示创建过程未完成或存在错误
  return RC::INTERNAL;
}
