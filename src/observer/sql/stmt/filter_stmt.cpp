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
#include "sql/stmt/filter_stmt.h"  // 包含过滤语句的头文件
#include "common/lang/string.h"  // 包含字符串操作相关的头文件
#include "common/log/log.h"  // 包含日志记录相关的头文件
#include "common/rc.h"  // 包含错误码定义的头文件
#include "storage/db/db.h"  // 包含数据库操作相关的头文件
#include "storage/table/table.h"  // 包含表操作相关的头文件

// FilterStmt类的析构函数，负责释放filter_units_列表中的FilterUnit对象
FilterStmt::~FilterStmt()
{
  for (FilterUnit *unit : filter_units_) {
    delete unit;  // 释放每个FilterUnit对象
  }
  filter_units_.clear();  // 清空列表
}

// 创建FilterStmt对象的静态方法
RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;  // 初始化错误码为成功
  stmt = nullptr;  // 初始化stmt为nullptr

  FilterStmt *tmp_stmt = new FilterStmt();  // 创建一个临时FilterStmt对象
  for (int i = 0; i < condition_num; i++) {  // 遍历条件列表
    FilterUnit *filter_unit = nullptr;  // 创建一个FilterUnit对象

    rc = create_filter_unit(db, default_table, tables, conditions[i], filter_unit);  // 创建过滤单元
    if (rc != RC::SUCCESS) {  // 如果创建失败
      delete tmp_stmt;  // 释放临时FilterStmt对象
      LOG_WARN("failed to create filter unit. condition index=%d", i);  // 记录警告日志
      return rc;  // 返回错误码
    }
    tmp_stmt->filter_units_.push_back(filter_unit);  // 将FilterUnit添加到列表中
  }

  stmt = tmp_stmt;  // 将临时FilterStmt对象赋值给stmt
  return rc;  // 返回成功状态码
}

// 获取表和字段的辅助方法
RC get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  // 如果关系名为空，使用默认表
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {  // 如果提供了表映射
    auto iter = tables->find(attr.relation_name);  // 查找表名
    if (iter != tables->end()) {  // 如果找到表
      table = iter->second;
    }
  } else {  // 如果没有提供表映射，使用数据库查找表
    table = db->find_table(attr.relation_name.c_str());
  }
  // 如果表不存在
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());  // 记录警告日志
    return RC::SCHEMA_TABLE_NOT_EXIST;  // 返回表不存在错误码
  }

  // 获取字段元数据
  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {  // 如果字段不存在
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());  // 记录警告日志
    table = nullptr;  // 将表指针设置为nullptr
    return RC::SCHEMA_FIELD_NOT_EXIST;  // 返回字段不存在错误码
  }

  return RC::SUCCESS;  // 返回成功状态码
}

// 创建FilterUnit对象的方法
RC FilterStmt::create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode &condition, FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;  // 初始化错误码为成功

  CompOp comp = condition.comp;  // 获取比较操作符
  if (comp < EQUAL_TO || comp >= NO_OP) {  // 如果操作符无效
    LOG_WARN("invalid compare operator : %d", comp);  // 记录警告日志
    return RC::INVALID_ARGUMENT;  // 返回无效参数错误码
  }

  filter_unit = new FilterUnit;  // 创建FilterUnit对象

  // 如果条件的左操作数是属性
  if (condition.left_is_attr) {
    Table           *table = nullptr;
    const FieldMeta *field = nullptr;
    rc                     = get_table_and_field(db, default_table, tables, condition.left_attr, table, field);  // 获取表和字段
    if (rc != RC::SUCCESS) {  // 如果获取失败
      LOG_WARN("cannot find attr");  // 记录警告日志
      return rc;  // 返回错误码
    }
    FilterObj filter_obj;  // 创建FilterObj对象
    filter_obj.init_attr(Field(table, field));  // 初始化为属性
    filter_unit->set_left(filter_obj);  // 设置左操作数
  } else {  // 如果条件的左操作数是值
    FilterObj filter_obj;  // 创建FilterObj对象
    filter_obj.init_value(condition.left_value);  // 初始化为值
    filter_unit->set_left(filter_obj);  // 设置左操作数
  }

  // 如果条件的右操作数是属性
  if (condition.right_is_attr) {
    Table           *table = nullptr;
    const FieldMeta *field = nullptr;
    rc                     = get_table_and_field(db, default_table, tables, condition.right_attr, table, field);  // 获取表和字段
    if (rc != RC::SUCCESS) {  // 如果获取失败
      LOG_WARN("cannot find attr");  // 记录警告日志
      return rc;  // 返回错误码
    }
    FilterObj filter_obj;  // 创建FilterObj对象
    filter_obj.init_attr(Field(table, field));  // 初始化为属性
    filter_unit->set_right(filter_obj);  // 设置右操作数
  } else {  // 如果条件的右操作数是值
    FilterObj filter_obj;  // 创建FilterObj对象
    filter_obj.init_value(condition.right_value);  // 初始化为值
    filter_unit->set_right(filter_obj);  // 设置右操作数
  }

  filter_unit->set_comp(comp);  // 设置比较操作符

  // 检查两个类型是否能够比较
  return rc;  // 返回成功状态码
}