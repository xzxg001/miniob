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
// Created by Wangyunlai on 2023/6/14.
//

#include "sql/stmt/desc_table_stmt.h"  // 包含描述表语句的头文件
#include "storage/db/db.h"  // 包含数据库操作相关的头文件

// 创建描述表语句的静态方法
RC DescTableStmt::create(Db *db, const DescTableSqlNode &desc_table, Stmt *&stmt)
{
  // 检查数据库中是否存在指定的表
  if (db->find_table(desc_table.relation_name.c_str()) == nullptr) {
    // 如果表不存在，返回表不存在的错误码
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  // 创建描述表语句对象，使用表名进行初始化
  stmt = new DescTableStmt(desc_table.relation_name);
  // 返回成功状态码
  return RC::SUCCESS;
}
