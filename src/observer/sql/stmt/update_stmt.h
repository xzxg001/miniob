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
#pragma once  // 防止头文件被重复包含

#include "common/rc.h"  // 包含错误码定义
#include "sql/stmt/stmt.h"  // 引入基础的SQL语句类定义

class Table;  // 声明Table类，表示数据库中的表

/**
 * @brief 代表更新语句的类
 * @ingroup Statement
 */
class UpdateStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 默认构造函数
  UpdateStmt() = default;

  // 构造函数，初始化更新语句所需的成员变量
  UpdateStmt(Table *table, Value *values, int value_amount);

public:
  // 静态方法，用于创建UpdateStmt对象
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  // 访问器方法，获取更新语句涉及的表对象
  Table *table() const { return table_; }

  // 访问器方法，获取更新语句中要设置的新值
  Value *values() const { return values_; }

  // 访问器方法，获取新值的数量
  int value_amount() const { return value_amount_; }

private:
  // 成员变量
  Table *table_        = nullptr;  // 指向更新语句涉及的表对象
  Value *values_       = nullptr;  // 指向更新语句中要设置的新值数组
  int value_amount_    = 0;        // 新值的数量
};
