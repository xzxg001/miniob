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
// Created by Wangyunlai on 2023/04/24.
// 

#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/value.h"
#include "storage/record/record.h"

void Field::set_int(Record &record, int value)
{
  // 确保字段类型为整型
  ASSERT(field_->type() == AttrType::INTS, "could not set int value to a non-int field");
  // 确保字段长度与整型大小匹配
  ASSERT(field_->len() == sizeof(value), "invalid field len");

  // 获取字段在记录中的数据位置
  char *field_data = record.data() + field_->offset();
  // 复制整型值到记录的字段数据中
  memcpy(field_data, &value, sizeof(value));
}

int Field::get_int(const Record &record)
{
  // 创建一个值对象，从记录中获取字段数据
  Value value(field_->type(), const_cast<char *>(record.data() + field_->offset()), field_->len());
  // 返回整型值
  return value.get_int();
}

const char *Field::get_data(const Record &record) 
{ 
  // 返回字段在记录中的数据指针
  return record.data() + field_->offset(); 
}
