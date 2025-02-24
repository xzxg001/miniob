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
// Created by Wangyunlai on 2023/7/12.
//


#include "sql/stmt/load_data_stmt.h"  // 导入LoadDataStmt类的声明
#include "common/lang/string.h"       // 导入字符串处理函数
#include "common/log/log.h"           // 导入日志系统
#include "storage/db/db.h"            // 导入数据库的声明
#include <unistd.h>                   // Unix标准函数库，用于访问权限检查

using namespace common;  // 使用common命名空间，简化代码

// LoadDataStmt类的create方法，用于创建LoadDataStmt对象
RC LoadDataStmt::create(Db *db, const LoadDataSqlNode &load_data, Stmt *&stmt)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 获取表名和文件名
  const char *table_name = load_data.relation_name.c_str();
  // 检查表名和文件名是否为空
  if (is_blank(table_name) || is_blank(load_data.file_name.c_str())) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, file name=%s",
        db, table_name, load_data.file_name.c_str());
    return RC::INVALID_ARGUMENT;  // 如果为空，记录日志并返回无效参数错误
  }

  // 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;  // 如果表不存在，记录日志并返回表不存在错误
  }

  // 检查文件是否存在且可读
  if (0 != access(load_data.file_name.c_str(), R_OK)) {
    LOG_WARN("no such file to load. file name=%s, error=%s", load_data.file_name.c_str(), strerror(errno));
    return RC::FILE_NOT_EXIST;  // 如果文件不存在或不可读，记录日志并返回文件不存在错误
  }

  // 创建LoadDataStmt对象并赋值给stmt
  stmt = new LoadDataStmt(table, load_data.file_name.c_str());
  return rc;  // 返回成功
}
