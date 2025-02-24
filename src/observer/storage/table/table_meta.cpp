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
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#include "common/lang/string.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/global_context.h"
#include "storage/table/table_meta.h"
#include "storage/trx/trx.h"
#include "json/json.h"

// JSON字段的静态字符串常量定义
static const Json::StaticString FIELD_TABLE_ID("table_id");
static const Json::StaticString FIELD_TABLE_NAME("table_name");
static const Json::StaticString FIELD_STORAGE_FORMAT("storage_format");
static const Json::StaticString FIELD_FIELDS("fields");
static const Json::StaticString FIELD_INDEXES("indexes");

// TableMeta类的构造函数，复制构造
TableMeta::TableMeta(const TableMeta &other)
    : table_id_(other.table_id_),         // 复制表ID
      name_(other.name_),                 // 复制表名
      fields_(other.fields_),             // 复制字段元数据
      indexes_(other.indexes_),           // 复制索引元数据
      storage_format_(other.storage_format_), // 复制存储格式
      record_size_(other.record_size_)    // 复制记录大小
{}

// 交换两个TableMeta对象的内容
void TableMeta::swap(TableMeta &other) noexcept
{
  name_.swap(other.name_);               // 交换表名
  fields_.swap(other.fields_);           // 交换字段元数据
  indexes_.swap(other.indexes_);         // 交换索引元数据
  std::swap(record_size_, other.record_size_); // 交换记录大小
}

// 初始化表元数据
RC TableMeta::init(int32_t table_id, const char *name, const std::vector<FieldMeta> *trx_fields,
                   span<const AttrInfoSqlNode> attributes, StorageFormat storage_format)
{
  // 检查表名是否为空
  if (common::is_blank(name)) {
    LOG_ERROR("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }

  // 检查属性是否有效
  if (attributes.size() == 0) {
    LOG_ERROR("Invalid argument. name=%s, field_num=%d", name, attributes.size());
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS; // 返回码初始化为成功

  int field_offset  = 0; // 字段偏移量
  int trx_field_num = 0; // 事务字段数量

  // 如果有事务字段，则初始化事务字段
  if (trx_fields != nullptr) {
    trx_fields_ = *trx_fields; // 复制事务字段

    fields_.resize(attributes.size() + trx_fields->size()); // 调整字段数组大小
    for (size_t i = 0; i < trx_fields->size(); i++) {
      const FieldMeta &field_meta = (*trx_fields)[i];
      // 初始化每个事务字段元数据
      fields_[i] = FieldMeta(field_meta.name(), field_meta.type(), field_offset, field_meta.len(), false /*visible*/, field_meta.field_id());
      field_offset += field_meta.len(); // 更新字段偏移量
    }

    trx_field_num = static_cast<int>(trx_fields->size()); // 设置事务字段数量
  } else {
    fields_.resize(attributes.size()); // 仅根据属性调整字段大小
  }

  // 初始化常规字段
  for (size_t i = 0; i < attributes.size(); i++) {
    const AttrInfoSqlNode &attr_info = attributes[i];
    rc = fields_[i + trx_field_num].init(
      attr_info.name.c_str(), attr_info.type, field_offset, attr_info.length, true /*visible*/, i);
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to init field meta. table name=%s, field name: %s", name, attr_info.name.c_str());
      return rc; // 返回错误
    }

    field_offset += attr_info.length; // 更新字段偏移量
  }

  record_size_ = field_offset; // 设置记录大小

  table_id_ = table_id; // 设置表ID
  name_     = name;     // 设置表名
  storage_format_ = storage_format; // 设置存储格式
  LOG_INFO("Sussessfully initialized table meta. table id=%d, name=%s", table_id, name);
  return RC::SUCCESS; // 返回成功
}

// 添加索引
RC TableMeta::add_index(const IndexMeta &index)
{
  indexes_.push_back(index); // 将索引添加到索引列表
  return RC::SUCCESS; // 返回成功
}

// 获取表名
const char *TableMeta::name() const { return name_.c_str(); }

// 获取事务字段
const FieldMeta *TableMeta::trx_field() const { return &fields_[0]; }

// 获取所有事务字段
span<const FieldMeta> TableMeta::trx_fields() const
{
  return span<const FieldMeta>(fields_.data(), sys_field_num()); // 返回事务字段的span
}

// 根据索引获取字段
const FieldMeta *TableMeta::field(int index) const { return &fields_[index]; }

// 根据字段名获取字段
const FieldMeta *TableMeta::field(const char *name) const
{
  if (nullptr == name) {
    return nullptr; // 如果名字为空，返回空指针
  }
  for (const FieldMeta &field : fields_) {
    if (0 == strcmp(field.name(), name)) {
      return &field; // 找到匹配的字段名，返回对应的字段
    }
  }
  return nullptr; // 未找到匹配字段，返回空指针
}

// 根据偏移量查找字段
const FieldMeta *TableMeta::find_field_by_offset(int offset) const
{
  for (const FieldMeta &field : fields_) {
    if (field.offset() == offset) {
      return &field; // 找到匹配的偏移量，返回对应字段
    }
  }
  return nullptr; // 未找到匹配字段，返回空指针
}

// 获取字段数量
int TableMeta::field_num() const { return fields_.size(); }

// 获取系统字段数量
int TableMeta::sys_field_num() const { return static_cast<int>(trx_fields_.size()); }

// 根据索引名获取索引
const IndexMeta *TableMeta::index(const char *name) const
{
  for (const IndexMeta &index : indexes_) {
    if (0 == strcmp(index.name(), name)) {
      return &index; // 找到匹配的索引名，返回对应的索引
    }
  }
  return nullptr; // 未找到匹配索引，返回空指针
}

// 根据字段名查找索引
const IndexMeta *TableMeta::find_index_by_field(const char *field) const
{
  for (const IndexMeta &index : indexes_) {
    if (0 == strcmp(index.field(), field)) {
      return &index; // 找到匹配的字段名，返回对应索引
    }
  }
  return nullptr; // 未找到匹配索引，返回空指针
}

// 根据索引获取索引
const IndexMeta *TableMeta::index(int i) const { return &indexes_[i]; }

// 获取索引数量
int TableMeta::index_num() const { return indexes_.size(); }

// 获取记录大小
int TableMeta::record_size() const { return record_size_; }

// 序列化表元数据到输出流
int TableMeta::serialize(std::ostream &ss) const
{
  Json::Value table_value; // 创建JSON对象
  table_value[FIELD_TABLE_ID]   = table_id_; // 设置表ID
  table_value[FIELD_TABLE_NAME] = name_; // 设置表名
  table_value[FIELD_STORAGE_FORMAT] = static_cast<int>(storage_format_); // 设置存储格式

  Json::Value fields_value; // 创建字段数组的JSON对象
  for (const FieldMeta &field : fields_) {
    Json::Value field_value;
    field.to_json(field_value); // 将字段元数据转换为JSON格式
    fields_value.append(std::move(field_value)); // 添加到字段数组
  }

  table_value[FIELD_FIELDS] = std::move(fields_value); // 设置字段数组到表对象

  Json::Value indexes_value; // 创建索引数组的JSON对象
  for (const auto &index : indexes_) {
    Json::Value index_value;
    index.to_json(index_value); // 将索引元数据转换为JSON格式
    indexes_value.append(std::move(index_value)); // 添加到索引数组
  }
  table_value[FIELD_INDEXES] = std::move(indexes_value); // 设置索引数组到表对象

  Json::StreamWriterBuilder builder; // 创建JSON写入器
  Json::StreamWriter       *writer = builder.newStreamWriter(); // 新建写入器

  std::streampos old_pos = ss.tellp(); // 获取当前输出流的位置
  writer->write(table_value, &ss); // 将表元数据写入输出流
  int ret = (int)(ss.tellp() - old_pos); // 计算写入字节数

  delete writer; // 释放写入器
  return ret; // 返回写入字节数
}

// 从输入流反序列化表元数据
int TableMeta::deserialize(std::istream &is)
{
  Json::Value             table_value; // 创建JSON对象
  Json::CharReaderBuilder builder; // 创建JSON读取器
  std::string             errors; // 错误信息

  std::streampos old_pos = is.tellg(); // 获取当前输入流的位置
  if (!Json::parseFromStream(builder, is, &table_value, &errors)) {
    LOG_ERROR("Failed to deserialize table meta. error=%s", errors.c_str());
    return -1; // 解析失败，返回错误
  }

  // 从JSON中读取表ID
  const Json::Value &table_id_value = table_value[FIELD_TABLE_ID];
  if (!table_id_value.isInt()) {
    LOG_ERROR("Invalid table id. json value=%s", table_id_value.toStyledString().c_str());
    return -1; // 表ID无效，返回错误
  }

  int32_t table_id = table_id_value.asInt(); // 读取表ID

  // 从JSON中读取表名
  const Json::Value &table_name_value = table_value[FIELD_TABLE_NAME];
  if (!table_name_value.isString()) {
    LOG_ERROR("Invalid table name. json value=%s", table_name_value.toStyledString().c_str());
    return -1; // 表名无效，返回错误
  }

  std::string table_name = table_name_value.asString(); // 读取表名

  // 从JSON中读取字段信息
  const Json::Value &fields_value = table_value[FIELD_FIELDS];
  if (!fields_value.isArray() || fields_value.size() <= 0) {
    LOG_ERROR("Invalid table meta. fields is not array, json value=%s", fields_value.toStyledString().c_str());
    return -1; // 字段信息无效，返回错误
  }

  // 从JSON中读取存储格式
  const Json::Value &storage_format_value = table_value[FIELD_STORAGE_FORMAT];
  if (!storage_format_value.isInt()) {
    LOG_ERROR("Invalid storage format. json value=%s", storage_format_value.toStyledString().c_str());
    return -1; // 存储格式无效，返回错误
  }

  int32_t storage_format = storage_format_value.asInt(); // 读取存储格式

  RC  rc        = RC::SUCCESS; // 返回码初始化为成功
  int field_num = fields_value.size(); // 获取字段数量

  std::vector<FieldMeta> fields(field_num); // 创建字段元数据数组
  for (int i = 0; i < field_num; i++) {
    FieldMeta &field = fields[i];

    const Json::Value &field_value = fields_value[i];
    rc = FieldMeta::from_json(field_value, field); // 从JSON中初始化字段元数据
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to deserialize table meta. table name =%s", table_name.c_str());
      return -1; // 反序列化失败，返回错误
    }
  }

  // 对字段进行排序，以偏移量为基准
  auto comparator = [](const FieldMeta &f1, const FieldMeta &f2) { return f1.offset() < f2.offset(); };
  std::sort(fields.begin(), fields.end(), comparator);

  // 更新表元数据
  table_id_ = table_id;
  storage_format_ = static_cast<StorageFormat>(storage_format);
  name_.swap(table_name);
  fields_.swap(fields);
  record_size_ = fields_.back().offset() + fields_.back().len() - fields_.begin()->offset(); // 计算记录大小

  // 初始化事务字段列表
  for (const FieldMeta &field_meta : fields_) {
    if (!field_meta.visible()) {
      trx_fields_.push_back(field_meta); // 仅添加可见性为false的字段
    }
  }

  // 处理索引信息
  const Json::Value &indexes_value = table_value[FIELD_INDEXES];
  if (!indexes_value.empty()) {
    if (!indexes_value.isArray()) {
      LOG_ERROR("Invalid table meta. indexes is not array, json value=%s", fields_value.toStyledString().c_str());
      return -1; // 索引信息无效，返回错误
    }
    const int              index_num = indexes_value.size(); // 获取索引数量
    std::vector<IndexMeta> indexes(index_num); // 创建索引元数据数组
    for (int i = 0; i < index_num; i++) {
      IndexMeta &index = indexes[i];

      const Json::Value &index_value = indexes_value[i];
      rc = IndexMeta::from_json(*this, index_value, index); // 从JSON中初始化索引元数据
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to deserialize table meta. table name=%s", table_name.c_str());
        return -1; // 反序列化失败，返回错误
      }
    }
    indexes_.swap(indexes); // 交换索引元数据
  }

  return (int)(is.tellg() - old_pos); // 返回读取字节数
}

// 获取序列化所需的大小（未实现）
int TableMeta::get_serial_size() const { return -1; }

// 将表元数据转换为字符串（未实现）
void TableMeta::to_string(std::string &output) const {}

// 输出表的描述信息
void TableMeta::desc(std::ostream &os) const
{
  os << name_ << '(' << std::endl; // 输出表名

  // 输出每个字段的描述
  for (const auto &field : fields_) {
    os << '\t';
    field.desc(os); // 输出字段的描述
    os << std::endl;
  }

  // 输出每个索引的描述
  for (const auto &index : indexes_) {
    os << '\t';
    index.desc(os); // 输出索引的描述
    os << std::endl;
  }
  os << ')' << std::endl; // 结束输出
}
