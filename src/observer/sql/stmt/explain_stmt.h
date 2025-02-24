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
// Created by Wangyunlai on 2022/12/27.
//
// 防止头文件被重复包含
#pragma once

#include "sql/stmt/stmt.h"  // 引入SQL语句基类的头文件

/**
 * @brief EXPLAIN语句类
 * @ingroup Statement
 * @details 这个类表示一个EXPLAIN语句，用于获取关于另一个SQL语句执行的详细信息。
 */
class ExplainStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，接受一个指向子语句的unique_ptr
  ExplainStmt(std::unique_ptr<Stmt> child_stmt) : child_stmt_(std::move(child_stmt)) {}

  // 默认的虚析构函数
  virtual ~ExplainStmt() = default;

  // 返回语句类型，这里是EXPLAIN
  StmtType type() const override { return StmtType::EXPLAIN; }

  // 提供对子语句的访问
  Stmt *child() const { return child_stmt_.get(); }

  // 静态方法，用于创建ExplainStmt对象
  static RC create(Db *db, const ExplainSqlNode &query, Stmt *&stmt);

private:
  // 成员变量，指向子语句的unique_ptr
  std::unique_ptr<Stmt> child_stmt_;
};
