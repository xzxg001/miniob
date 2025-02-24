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
// Created by Wangyunlai on 2023/4/25.
//
#include "sql/stmt/create_index_stmt.h"  // 包含创建索引语句的头文件
#include "common/lang/string.h"  // 包含字符串操作相关的头文件
#include "common/log/log.h"  // 包含日志记录相关的头文件
#include "storage/db/db.h"  // 包含数据库操作相关的头文件
#include "storage/table/table.h"  // 包含表操作相关的头文件

using namespace std;  // 使用标准命名空间
using namespace common;  // 使用common命名空间，可能包含一些通用的工具类和函数

/**
 * 创建索引的语句实现。
 * @param db 指向数据库对象的指针
 * @param create_index 包含创建索引所需信息的SQL节点
 * @param stmt 输出参数，指向创建的语句对象的指针
 * @return 操作结果，成功或错误码
 */
RC CreateIndexStmt::create(Db *db, const CreateIndexSqlNode &create_index, Stmt *&stmt)
{
  stmt = nullptr;  // 初始化stmt为nullptr

  // 获取表名、索引名和属性名
  const char *table_name = create_index.relation_name.c_str();
  if (is_blank(table_name) || is_blank(create_index.index_name.c_str()) ||
      is_blank(create_index.attribute_name.c_str())) {
    // 如果任何一个参数为空，记录警告日志并返回无效参数错误码
    LOG_WARN("invalid argument. db=%p, table_name=%p, index name=%s, attribute name=%s",
        db, table_name, create_index.index_name.c_str(), create_index.attribute_name.c_str());
    return RC::INVALID_ARGUMENT;
  }

  // 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    // 如果表不存在，记录警告日志并返回表不存在错误码
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 获取字段元数据
  const FieldMeta *field_meta = table->table_meta().field(create_index.attribute_name.c_str());
  if (nullptr == field_meta) {
    // 如果字段不存在，记录警告日志并返回字段不存在错误码
    LOG_WARN("no such field in table. db=%s, table=%s, field name=%s", 
             db->name(), table_name, create_index.attribute_name.c_str());
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 检查索引是否已存在
  Index *index = table->find_index(create_index.index_name.c_str());
  if (nullptr != index) {
    // 如果索引已存在，记录警告日志并返回索引名重复错误码
    LOG_WARN("index with name(%s) already exists. table name=%s", create_index.index_name.c_str(), table_name);
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  // 创建创建索引语句对象并返回成功
  stmt = new CreateIndexStmt(table, field_meta, create_index.index_name);
  return RC::SUCCESS;
}