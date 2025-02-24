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
// Created by Meiyi
//

#pragma once  // 确保头文件内容只被包含一次，防止重复包含

#include "common/rc.h"  // 包含通用的错误码定义
#include "sql/parser/parse_defs.h"  // 包含SQL解析相关的类型和宏定义

// 声明一个函数，用于解析SQL语句
// 参数：
//   - const char *st: 指向包含SQL语句的字符串的指针
//   - ParsedSqlResult *sql_result: 指向用于存储解析结果的结构的指针
// 返回值：
//   - RC: 表示操作结果的错误码
RC parse(const char *st, ParsedSqlResult *sql_result);