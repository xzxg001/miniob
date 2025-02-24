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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name"); // 字段名称
const static Json::StaticString FIELD_FIELD_NAME("field_name"); // 字段字段名

RC IndexMeta::init(const char *name, const FieldMeta &field)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty."); // 初始化索引失败，名称为空
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  name_  = name; // 设置索引名称
  field_ = field.name(); // 设置字段名
  return RC::SUCCESS; // 返回成功
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME]       = name_; // 将索引名称写入JSON
  json_value[FIELD_FIELD_NAME] = field_; // 将字段名写入JSON
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME]; // 获取索引名称
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME]; // 获取字段名称
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str()); // 索引名称不是字符串
    return RC::INTERNAL; // 返回内部错误
  }

  if (!field_value.isString()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(), field_value.toStyledString().c_str()); // 字段名称不是字符串
    return RC::INTERNAL; // 返回内部错误
  }

  const FieldMeta *field = table.field(field_value.asCString()); // 从表元数据中获取字段
  if (nullptr == field) {
    LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString()); // 反序列化索引时字段不存在
    return RC::SCHEMA_FIELD_MISSING; // 返回字段缺失错误
  }

  return index.init(name_value.asCString(), *field); // 初始化索引
}

const char *IndexMeta::name() const { return name_.c_str(); } // 返回索引名称

const char *IndexMeta::field() const { return field_.c_str(); } // 返回字段名称

void IndexMeta::desc(ostream &os) const { os << "index name=" << name_ << ", field=" << field_; } // 描述索引
