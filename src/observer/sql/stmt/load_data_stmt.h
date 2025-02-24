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

#pragma once  // 预处理指令，确保头文件只被包含一次

#include <string>  // 引入C++标准库中的string类

#include "sql/stmt/stmt.h"  // 引入Stmt基类的声明

class Table;  // 表类的前向声明

class LoadDataStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，初始化表对象和文件名
  LoadDataStmt(Table *table, const char *filename) : table_(table), filename_(filename) {}
  virtual ~LoadDataStmt() = default;  // 虚析构函数，允许派生类正确析构

  // 返回语句类型，这里是加载数据语句
  StmtType type() const override { return StmtType::LOAD_DATA; }

  // 获取表对象的访问器
  Table *table() const { return table_; }
  
  // 获取文件名的访问器
  const char *filename() const { return filename_.c_str(); }

  // 静态方法，用于创建LoadDataStmt对象
  static RC create(Db *db, const LoadDataSqlNode &load_data, Stmt *&stmt);

private:
  // 表对象指针
  Table *table_ = nullptr;
  
  // 要加载的文件名
  std::string filename_;
};