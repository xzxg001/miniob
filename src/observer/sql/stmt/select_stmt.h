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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once  // 预处理指令，确保头文件只被包含一次

#include <memory>  // 引入智能指针相关的库
#include <vector>  // 引入向量容器库

#include "common/rc.h"  // 引入返回码定义
#include "sql/stmt/stmt.h"  // 引入基类Stmt的声明
#include "storage/field/field.h"  // 引入字段相关的声明

class FieldMeta;  // 字段元数据类的前向声明
class FilterStmt;  // 过滤语句类的前向声明
class Db;  // 数据库类的前向声明
class Table;  // 表类的前向声明

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt  // 继承自Stmt基类
{
public:
  SelectStmt() = default;  // 默认构造函数
  ~SelectStmt() override;  // 析构函数，用于释放资源

  StmtType type() const override { return StmtType::SELECT; }  // 返回语句类型，这里是SELECT

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt);  // 静态方法，用于创建SelectStmt对象

public:
  const std::vector<Table *> &tables() const { return tables_; }  // 获取表对象向量的访问器
  FilterStmt *filter_stmt() const { return filter_stmt_; }  // 获取过滤语句对象的访问器

  std::vector<std::unique_ptr<Expression>> &query_expressions() { return query_expressions_; }  // 获取查询表达式向量的访问器
  std::vector<std::unique_ptr<Expression>> &group_by() { return group_by_; }  // 获取GROUP BY子句中表达式向量的访问器

private:
  std::vector<std::unique_ptr<Expression>> query_expressions_;  // 存储查询表达式的向量
  std::vector<Table *> tables_;  // 存储表对象的向量
  FilterStmt *filter_stmt_ = nullptr;  // 指向过滤语句对象的指针
  std::vector<std::unique_ptr<Expression>> group_by_;  // 存储GROUP BY子句中表达式的向量
};