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
// Created by WangYunlai on 2022/6/7.
//

// 防止头文件重复包含
#pragma once

#include "storage/field/field_meta.h"  // 引入字段元数据的头文件
#include <iostream>                    // 引入输入输出流库

// TupleCellSpec 类用于描述一个元组单元的规格
class TupleCellSpec final
{
public:
  // 默认构造函数
  TupleCellSpec() = default;

  // 构造函数，接受表名、字段名和可选别名参数
  TupleCellSpec(const char *table_name, const char *field_name, const char *alias = nullptr);

  // 构造函数，接受别名参数
  explicit TupleCellSpec(const char *alias);

  // 构造函数，接受 std::string 类型的别名参数
  explicit TupleCellSpec(const std::string &alias);

  // 获取表名
  const char *table_name() const { return table_name_.c_str(); }

  // 获取字段名
  const char *field_name() const { return field_name_.c_str(); }

  // 获取别名
  const char *alias() const { return alias_.c_str(); }

  // 比较当前对象和其他对象是否相等
  // @param other 另一个 TupleCellSpec 对象
  // @return 如果当前对象与其他对象的表名、字段名和别名都相同，则返回 true，否则返回 false
  bool equals(const TupleCellSpec &other) const
  {
    return table_name_ == other.table_name_ &&  // 比较表名是否相同
           field_name_ == other.field_name_ &&  // 比较字段名是否相同
           alias_ == other.alias_;              // 比较别名是否相同
  }

private:
  std::string table_name_;  // 表名
  std::string field_name_;  // 字段名
  std::string alias_;       // 别名
};  // 类结束
