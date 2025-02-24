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
// Created by Wangyunlai on 2023/6/13.
//
// 包含日志记录相关的头文件
#include "common/log/log.h"
// 包含类型定义的头文件
#include "common/types.h"
// 包含创建表语句的头文件
#include "sql/stmt/create_table_stmt.h"
// 包含SQL调试事件的头文件
#include "event/sql_debug.h"

// 创建表语句的实现
RC CreateTableStmt::create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt)
{
  // 初始化存储格式为未知
  StorageFormat storage_format = StorageFormat::UNKNOWN_FORMAT;
  // 如果没有指定存储格式，则默认使用行格式
  if (create_table.storage_format.length() == 0) {
    storage_format = StorageFormat::ROW_FORMAT;
  } else {
    // 否则，根据字符串解析存储格式
    storage_format = get_storage_format(create_table.storage_format.c_str());
  }
  // 如果存储格式为未知，则返回无效参数错误码
  if (storage_format == StorageFormat::UNKNOWN_FORMAT) {
    return RC::INVALID_ARGUMENT;
  }
  // 创建创建表语句对象
  stmt = new CreateTableStmt(create_table.relation_name, create_table.attr_infos, storage_format);
  // 输出SQL调试信息
  sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
  // 返回成功状态码
  return RC::SUCCESS;
}

// 获取存储格式的静态方法
StorageFormat CreateTableStmt::get_storage_format(const char *format_str) {
  // 初始化存储格式为未知
  StorageFormat format = StorageFormat::UNKNOWN_FORMAT;
  // 如果字符串为"ROW"（不区分大小写），则设置为行格式
  if (0 == strcasecmp(format_str, "ROW")) {
    format = StorageFormat::ROW_FORMAT;
  // 如果字符串为"PAX"（不区分大小写），则设置为PAX格式
  } else if (0 == strcasecmp(format_str, "PAX")) {
    format = StorageFormat::PAX_FORMAT;
  // 如果不是已知的存储格式，保持为未知
  } else {
    format = StorageFormat::UNKNOWN_FORMAT;
  }
  // 返回解析后的存储格式
  return format;
}