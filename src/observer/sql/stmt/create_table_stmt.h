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
// 防止头文件被重复包含
#pragma once

#include <string>  // 引入标准库中的字符串类
#include <vector>  // 引入标准库中的向量容器

// 引入项目中定义的其他头文件
#include "sql/stmt/stmt.h"  // 引入SQL语句基类的头文件

class Db;  // 声明外部依赖的数据库类

/**
 * @brief 表示创建表的语句
 * @ingroup Statement
 * @details 虽然解析成了stmt，但是与原始的SQL解析后的数据也差不多
 */
class CreateTableStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化表名、属性信息列表和存储格式
  CreateTableStmt(
      const std::string &table_name, const std::vector<AttrInfoSqlNode> &attr_infos, StorageFormat storage_format)
      : table_name_(table_name), attr_infos_(attr_infos), storage_format_(storage_format)
  {}
  // 默认的虚析构函数
  virtual ~CreateTableStmt() = default;

  // 返回语句类型，这里是创建表
  StmtType type() const override { return StmtType::CREATE_TABLE; }

  // 提供对表名的访问
  const std::string &table_name() const { return table_name_; }

  // 提供对属性信息列表的访问
  const std::vector<AttrInfoSqlNode> &attr_infos() const { return attr_infos_; }

  // 提供对存储格式的访问
  const StorageFormat storage_format() const { return storage_format_; }

  // 静态方法，用于创建CreateTableStmt对象
  static RC create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt);

  // 静态方法，用于根据字符串解析存储格式
  static StorageFormat get_storage_format(const char *format_str);

private:
  // 成员变量
  std::string table_name_;  // 表名
  std::vector<AttrInfoSqlNode> attr_infos_;  // 属性信息列表
  StorageFormat storage_format_;  // 存储格式
};