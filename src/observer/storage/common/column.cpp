/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/log/log.h"
#include "storage/common/column.h"

// 构造函数：根据字段元数据和指定大小初始化列
Column::Column(const FieldMeta &meta, size_t size)
    : data_(nullptr),
      count_(0),
      capacity_(0),
      own_(true),
      attr_type_(meta.type()),
      attr_len_(meta.len()),
      column_type_(Type::NORMAL_COLUMN)
{
  // TODO: 如果不需要分配内存，可以优化内存使用
  data_     = new char[size * attr_len_]; // 分配内存
  capacity_ = size; // 设置容量
}

// 构造函数：根据属性类型、长度和容量初始化列
Column::Column(AttrType attr_type, int attr_len, size_t capacity)
{
  attr_type_   = attr_type; // 设置属性类型
  attr_len_    = attr_len;   // 设置属性长度
  data_        = new char[capacity * attr_len_]; // 分配内存
  count_       = 0; // 初始化计数为 0
  capacity_    = capacity; // 设置容量
  own_         = true; // 设置为拥有内存
  column_type_ = Type::NORMAL_COLUMN; // 设置列类型
}

// 根据字段元数据和指定大小初始化列
void Column::init(const FieldMeta &meta, size_t size)
{
  reset(); // 重置当前列
  data_        = new char[size * meta.len()]; // 分配内存
  count_       = 0; // 初始化计数为 0
  capacity_    = size; // 设置容量
  attr_type_   = meta.type(); // 设置属性类型
  attr_len_    = meta.len();   // 设置属性长度
  own_         = true; // 设置为拥有内存
  column_type_ = Type::NORMAL_COLUMN; // 设置列类型
}

// 根据属性类型、长度和容量初始化列
void Column::init(AttrType attr_type, int attr_len, size_t capacity)
{
  reset(); // 重置当前列
  data_        = new char[capacity * attr_len]; // 分配内存
  count_       = 0; // 初始化计数为 0
  capacity_    = capacity; // 设置容量
  own_         = true; // 设置为拥有内存
  attr_type_   = attr_type; // 设置属性类型
  attr_len_    = attr_len;   // 设置属性长度
  column_type_ = Type::NORMAL_COLUMN; // 设置列类型
}

// 根据值初始化列
void Column::init(const Value &value)
{
  reset(); // 重置当前列
  attr_type_ = value.attr_type(); // 设置属性类型
  attr_len_  = value.length(); // 设置属性长度
  data_      = new char[attr_len_]; // 分配内存
  count_     = 1; // 设置计数为 1
  capacity_  = 1; // 设置容量为 1
  own_       = true; // 设置为拥有内存
  memcpy(data_, value.data(), attr_len_); // 拷贝值的数据
  column_type_ = Type::CONSTANT_COLUMN; // 设置列类型为常量列
}

// 重置列，释放内存
void Column::reset()
{
  if (data_ != nullptr && own_) {
    delete[] data_; // 释放内存
  }
  data_ = nullptr; // 设置数据为空
  count_       = 0; // 重置计数
  capacity_    = 0; // 重置容量
  own_         = false; // 设置为不拥有内存
  attr_type_   = AttrType::UNDEFINED; // 设置属性类型为未定义
  attr_len_    = -1; // 设置属性长度为 -1
}

// 向列中追加一个值
RC Column::append_one(char *data) { return append(data, 1); }

// 向列中追加多个值
RC Column::append(char *data, int count)
{
  if (!own_) {
    LOG_WARN("append data to non-owned column"); // 警告：向非拥有的列追加数据
    return RC::INTERNAL;
  }
  if (count_ + count > capacity_) {
    LOG_WARN("append data to full column"); // 警告：向已满的列追加数据
    return RC::INTERNAL;
  }

  memcpy(data_ + count_ * attr_len_, data, count * attr_len_); // 拷贝数据
  count_ += count; // 更新计数
  return RC::SUCCESS; // 返回成功
}

// 获取指定索引的值
Value Column::get_value(int index) const
{
  if (index >= count_ || index < 0) {
    return Value(); // 如果索引无效，返回默认值
  }
  return Value(attr_type_, &data_[index * attr_len_], attr_len_); // 返回指定索引的值
}

// 引用另一个列的内容
void Column::reference(const Column &column)
{
  if (this == &column) {
    return; // 防止自我引用
  }
  reset(); // 重置当前列

  this->data_     = column.data(); // 引用数据
  this->capacity_ = column.capacity(); // 设置容量
  this->count_    = column.count(); // 设置计数
  this->own_      = false; // 设置为不拥有内存

  this->column_type_ = column.column_type(); // 引用列类型
  this->attr_type_   = column.attr_type(); // 引用属性类型
  this->attr_len_    = column.attr_len(); // 引用属性长度
}
