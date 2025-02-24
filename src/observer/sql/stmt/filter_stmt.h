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
// Created by Wangyunlai on 2022/5/22.
//
// 防止头文件被重复包含
#pragma once

#include "sql/expr/expression.h"  // 包含表达式相关的头文件
#include "sql/parser/parse_defs.h"  // 包含解析定义的头文件
#include "sql/stmt/stmt.h"  // 包含SQL语句基类的头文件
#include <unordered_map>  // 引入标准库中的无序映射容器
#include <vector>  // 引入标准库中的向量容器

class Db;  // 数据库类声明
class Table;  // 表类声明
class FieldMeta;  // 字段元数据类声明

// 过滤对象结构体，用于表示过滤条件中的属性或值
struct FilterObj
{
  bool  is_attr;         // 是否是属性
  Field field;          // 字段信息，当is_attr为true时使用
  Value value;          // 值信息，当is_attr为false时使用

  // 初始化为属性
  void init_attr(const Field &field)
  {
    is_attr = true;
    this->field = field;
  }

  // 初始化为值
  void init_value(const Value &value)
  {
    is_attr = false;
    this->value = value;
  }
};

// 过滤单元类，表示一个条件表达式，如 "field = value"
class FilterUnit
{
public:
  FilterUnit() = default;  // 默认构造函数
  ~FilterUnit() {}  // 析构函数

  // 设置比较操作符
  void set_comp(CompOp comp) { comp_ = comp; }

  // 获取比较操作符
  CompOp comp() const { return comp_; }

  // 设置左操作数
  void set_left(const FilterObj &obj) { left_ = obj; }

  // 设置右操作数
  void set_right(const FilterObj &obj) { right_ = obj; }

  // 获取左操作数
  const FilterObj &left() const { return left_; }

  // 获取右操作数
  const FilterObj &right() const { return right_; }

private:
  CompOp comp_ = NO_OP;  // 比较操作符，默认为无操作
  FilterObj left_;      // 左操作数
  FilterObj right_;     // 右操作数
};

/**
 * @brief 过滤语句类
 * @ingroup Statement
 */
class FilterStmt
{
public:
  FilterStmt() = default;  // 默认构造函数
  virtual ~FilterStmt();   // 虚析构函数

  // 获取过滤单元列表
  const std::vector<FilterUnit *> &filter_units() const { return filter_units_; }

  // 静态方法，用于创建FilterStmt对象
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt);

  // 静态方法，用于创建FilterUnit对象
  static RC create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const ConditionSqlNode &condition, FilterUnit *&filter_unit);

private:
  std::vector<FilterUnit *> filter_units_;  // 过滤单元列表，默认当前都是AND关系
};