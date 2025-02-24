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

// 防止头文件被重复包含
#pragma once

#include <string>  // 引入标准库中的字符串类

// 引入SQL语句基类的头文件
#include "sql/stmt/stmt.h"

class Db;  // 声明外部依赖的数据库类

/**
 * @brief 描述表的语句类
 * @ingroup Statement
 * @details 虽然解析成了stmt，但是与原始的SQL解析后的数据也差不多
 */
class DescTableStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化表名
  DescTableStmt(const std::string &table_name) : table_name_(table_name) {}
  // 默认的虚析构函数
  virtual ~DescTableStmt() = default;

  // 返回语句类型，这里是描述表语句
  StmtType type() const override { return StmtType::DESC_TABLE; }

  // 提供对表名的只读访问
  const std::string &table_name() const { return table_name_; }

  // 静态方法，用于创建DescTableStmt对象
  static RC create(Db *db, const DescTableSqlNode &desc_table, Stmt *&stmt);

private:
  // 成员变量
  std::string table_name_;  // 表名
};