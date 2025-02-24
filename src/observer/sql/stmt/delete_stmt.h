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
// Created by Wangyunlai on 2022/5/27.
//
// 防止头文件被重复包含
#pragma once

// 引入SQL解析相关的公共定义
#include "sql/parser/parse_defs.h"
// 引入SQL语句基类的头文件
#include "sql/stmt/stmt.h"

class Table;  // 声明外部依赖的表类
class FilterStmt;  // 声明外部依赖的过滤语句类

/**
 * @brief 删除语句类
 * @ingroup Statement
 */
class DeleteStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化表对象和过滤语句对象
  DeleteStmt(Table *table, FilterStmt *filter_stmt);
  // 虚析构函数，确保派生类的析构函数被正确调用
  ~DeleteStmt() override;

  // 提供对表对象的只读访问
  Table *table() const { return table_; }
  // 提供对过滤语句对象的只读访问
  FilterStmt *filter_stmt() const { return filter_stmt_; }

  // 返回语句类型，这里是删除语句
  StmtType type() const override { return StmtType::DELETE; }

public:
  // 静态方法，用于创建DeleteStmt对象
  static RC create(Db *db, const DeleteSqlNode &delete_sql, Stmt *&stmt);

private:
  // 成员变量
  Table *table_;       // 指向表对象的指针
  FilterStmt *filter_stmt_;  // 指向过滤语句对象的指针
};