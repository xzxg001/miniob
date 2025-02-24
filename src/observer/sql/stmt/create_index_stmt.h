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
// 防止头文件被重复包含
#pragma once

#include <string>  // 引入标准库中的字符串类

// 引入项目中定义的其他头文件
#include "sql/stmt/stmt.h"  // 引入SQL语句基类的头文件

// 声明外部依赖的结构体和类
struct CreateIndexSqlNode;  // 创建索引的SQL节点结构体
class Table;  // 表类
class FieldMeta;  // 字段元数据类

/**
 * @brief 创建索引的语句
 * @ingroup Statement
 */
class CreateIndexStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化表对象、字段元数据和索引名
  CreateIndexStmt(Table *table, const FieldMeta *field_meta, const std::string &index_name)
      : table_(table), field_meta_(field_meta), index_name_(index_name) {}

  // 默认的虚析构函数
  virtual ~CreateIndexStmt() = default;

  // 返回语句类型，这里是创建索引
  StmtType type() const override { return StmtType::CREATE_INDEX; }

  // 提供对表对象的访问
  Table *table() const { return table_; }

  // 提供对字段元数据的访问
  const FieldMeta *field_meta() const { return field_meta_; }

  // 提供对索引名的访问
  const std::string &index_name() const { return index_name_; }

public:
  // 静态方法，用于创建CreateIndexStmt对象
  static RC create(Db *db, const CreateIndexSqlNode &create_index, Stmt *&stmt);

private:
  // 成员变量
  Table *table_;      // 指向表对象的指针
  const FieldMeta *field_meta_;  // 指向字段元数据的指针
  std::string index_name_;  // 索引名
};
