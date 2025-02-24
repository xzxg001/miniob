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
#include "sql/stmt/explain_stmt.h"  // 包含解释语句的头文件
#include "common/log/log.h"  // 包含日志记录相关的头文件
#include "sql/stmt/stmt.h"  // 包含SQL语句基类的头文件

// 解释语句类的构造函数
ExplainStmt::ExplainStmt(std::unique_ptr<Stmt> child_stmt) : child_stmt_(std::move(child_stmt)) {}

// 创建解释语句的静态方法
RC ExplainStmt::create(Db *db, const ExplainSqlNode &explain, Stmt *&stmt)
{
  Stmt *child_stmt = nullptr;  // 创建一个指向子语句的指针
  RC rc = Stmt::create_stmt(db, *explain.sql_node, child_stmt);  // 调用Stmt类的create_stmt方法创建子语句
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child stmt. rc=%s", strrc(rc));  // 如果创建失败，记录警告日志
    return rc;  // 返回错误码
  }

  // 创建一个指向子语句的智能指针
  std::unique_ptr<Stmt> child_stmt_ptr = std::unique_ptr<Stmt>(child_stmt);
  // 创建解释语句对象，并将子语句的智能指针传递给构造函数
  stmt = new ExplainStmt(std::move(child_stmt_ptr));
  return rc;  // 返回创建子语句时的状态码
}