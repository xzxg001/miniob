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
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"  // 引入 TupleCellSpec 类的头文件
#include "common/lang/string.h"   // 引入字符串操作的头文件

using namespace std;  // 使用标准命名空间

// TupleCellSpec 构造函数，接受表名、字段名和可选别名参数
TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias)
{
  // 如果 table_name 不为空，则将其赋值给成员变量 table_name_
  if (table_name) {
    table_name_ = table_name;  // 初始化表名
  }
  // 如果 field_name 不为空，则将其赋值给成员变量 field_name_
  if (field_name) {
    field_name_ = field_name;  // 初始化字段名
  }
  // 如果 alias 不为空，则将其赋值给成员变量 alias_
  if (alias) {
    alias_ = alias;  // 初始化别名
  } else {
    // 如果别名为空且表名为空，则将字段名赋值给别名
    if (table_name_.empty()) {
      alias_ = field_name_;  // 使用字段名作为别名
    } else {
      // 如果表名不为空，则将表名和字段名组合赋值给别名
      alias_ = table_name_ + "." + field_name_;  // 使用“表名.字段名”格式作为别名
    }
  }
}

// TupleCellSpec 构造函数，接受别名参数
TupleCellSpec::TupleCellSpec(const char *alias)
{
  // 如果 alias 不为空，则将其赋值给成员变量 alias_
  if (alias) {
    alias_ = alias;  // 初始化别名
  }
}

// TupleCellSpec 构造函数，接受 std::string 类型的别名参数
TupleCellSpec::TupleCellSpec(const string &alias) : alias_(alias)  // 使用成员初始化列表直接初始化别名
{}
