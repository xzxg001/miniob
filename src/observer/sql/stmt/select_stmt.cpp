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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"  // 导入SelectStmt类的声明
#include "common/lang/string.h"     // 导入字符串处理函数
#include "common/log/log.h"        // 导入日志系统
#include "sql/stmt/filter_stmt.h"  // 导入FilterStmt类的声明
#include "storage/db/db.h"         // 导入数据库的声明
#include "storage/table/table.h"   // 导入表的声明
#include "sql/parser/expression_binder.h"  // 导入表达式绑定器的声明

using namespace std;    // 使用标准命名空间，简化代码
using namespace common;  // 使用common命名空间，简化代码

SelectStmt::~SelectStmt()  // 析构函数，用于释放资源
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;  // 删除filter_stmt_指向的FilterStmt对象
    filter_stmt_ = nullptr;  // 将指针设置为nullptr
  }
}

// 创建SelectStmt对象的方法
RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");  // 日志记录db为空的警告
    return RC::INVALID_ARGUMENT;  // 返回无效参数错误码
  }

  BinderContext binder_context;  // 创建一个绑定上下文对象

  // 收集FROM子句中的表
  vector<Table *>                tables;  // 存储表对象的向量
  unordered_map<string, Table *> table_map;  // 存储表名和表对象的映射
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();  // 获取表名
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);  // 日志记录表名为空的警告
      return RC::INVALID_ARGUMENT;  // 返回无效参数错误码
    }

    Table *table = db->find_table(table_name);  // 在数据库中查找表
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);  // 日志记录表不存在的警告
      return RC::SCHEMA_TABLE_NOT_EXIST;  // 返回表不存在错误码
    }

    binder_context.add_table(table);  // 将表添加到绑定上下文中
    tables.push_back(table);  // 将表添加到向量中
    table_map.insert({table_name, table});  // 将表名和表对象添加到映射中
  }

  // 收集SELECT子句中的查询字段
  vector<unique_ptr<Expression>> bound_expressions;  // 存储绑定后的表达式
  ExpressionBinder expression_binder(binder_context);  // 创建表达式绑定器对象
  
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);  // 绑定表达式
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));  // 日志记录绑定表达式失败的信息
      return rc;  // 返回错误码
    }
  }

  vector<unique_ptr<Expression>> group_by_expressions;  // 存储GROUP BY子句中的表达式
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);  // 绑定表达式
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));  // 日志记录绑定表达式失败的信息
      return rc;  // 返回错误码
    }
  }

  Table *default_table = nullptr;  // 默认表对象指针
  if (tables.size() == 1) {
    default_table = tables[0];  // 如果只有一个表，则设置为默认表
  }

  // 创建WHERE子句中的过滤语句
  FilterStmt *filter_stmt = nullptr;  // 过滤语句对象指针
  RC          rc          = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);  // 创建过滤语句
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");  // 日志记录无法构造过滤语句的警告
    return rc;  // 返回错误码
  }

  // 一切正常，创建SelectStmt对象
  SelectStmt *select_stmt = new SelectStmt();  // 创建SelectStmt对象

  select_stmt->tables_.swap(tables);  // 交换表向量
  select_stmt->query_expressions_.swap(bound_expressions);  // 交换查询表达式向量
  select_stmt->filter_stmt_ = filter_stmt;  // 设置过滤语句对象
  select_stmt->group_by_.swap(group_by_expressions);  // 交换GROUP BY子句中的表达式向量
  stmt                      = select_stmt;  // 将SelectStmt对象赋值给stmt
  return RC::SUCCESS;  // 返回成功
}