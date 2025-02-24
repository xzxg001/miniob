/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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
// 引入必要的头文件
#include "sql/stmt/insert_stmt.h"  // 插入语句的声明
#include "common/log/log.h"        // 日志系统的声明
#include "storage/db/db.h"          // 数据库的声明
#include "storage/table/table.h"    // 表的声明

// 插入语句的构造函数
// @table: 要插入数据的表
// @values: 要插入的值数组
// @value_amount: 值的数量
InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

// 创建插入语句的函数
// @db: 数据库实例
// @inserts: 插入的SQL节点，包含要插入的表名和值
// @stmt: 创建的插入语句对象的引用
RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  // 获取要插入数据的表名
  const char *table_name = inserts.relation_name.c_str();
  
  // 参数校验，确保数据库实例、表名和值都不为空
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    // 如果参数无效，记录警告日志，并返回无效参数错误码
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    // 如果表不存在，记录警告日志，并返回表不存在错误码
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 检查字段数量是否匹配
  const Value     *values     = inserts.values.data();     // 要插入的值数组
  const int        value_num  = static_cast<int>(inserts.values.size());  // 值的数量
  const TableMeta &table_meta = table->table_meta();       // 表的元数据
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();  // 表的字段数量（不包括系统字段）
  if (field_num != value_num) {
    // 如果字段数量不匹配，记录警告日志，并返回字段缺失错误码
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // 如果一切正常，创建插入语句对象，并返回成功码
  stmt = new InsertStmt(table, values, value_num);
  return RC::SUCCESS;
}