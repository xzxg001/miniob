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
#pragma once  // 预处理指令，确保头文件只被包含一次，防止重复定义

#include "common/rc.h"  // 引入错误码定义
#include "sql/stmt/stmt.h"  // 引入语句基类Stmt

class Table;  // 表类的前向声明
class Db;     // 数据库类的前向声明

/**
 * @brief 插入语句类
 * @ingroup Statement
 */
class InsertStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 默认构造函数
  InsertStmt() = default;

  // 带参数的构造函数
  // @table: 要插入数据的表
  // @values: 要插入的值数组
  // @value_amount: 值的数量
  InsertStmt(Table *table, const Value *values, int value_amount);

  // 返回语句类型，这里是插入语句
  StmtType type() const override { return StmtType::INSERT; }

public:
  // 静态方法，用于创建插入语句对象
  // @db: 数据库实例
  // @insert_sql: 包含插入SQL语句信息的节点
  // @stmt: 创建的插入语句对象的引用
  static RC create(Db *db, const InsertSqlNode &insert_sql, Stmt *&stmt);

public:
  // 获取插入语句相关的表对象
  Table *table() const { return table_; }

  // 获取要插入的值数组
  const Value *values() const { return values_; }

  // 获取要插入的值的数量
  int value_amount() const { return value_amount_; }

private:
  // 插入语句相关的表对象指针
  Table *table_ = nullptr;

  // 要插入的值数组指针
  const Value *values_ = nullptr;

  // 要插入的值的数量
  int value_amount_ = 0;
};
