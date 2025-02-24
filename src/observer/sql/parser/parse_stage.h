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
// Created by Longda on 2021/4/13.
//

#pragma once  // 确保头文件内容只被包含一次，防止重复包含

#include "common/rc.h"  // 包含通用的错误码定义

class SQLStageEvent;  // 声明SQLStageEvent类，表示SQL处理阶段的事件

/**
 * @brief 解析SQL语句，解析后的结果可以参考parse_defs.h
 * @ingroup SQLStage
 */
class ParseStage
{
public:
  // 成员函数，用于处理SQL事件的请求
  // 参数：
  //   - SQLStageEvent *sql_event: 指向SQLStageEvent对象的指针，包含需要解析的SQL语句
  // 返回值：
  //   - RC: 表示操作结果的错误码
  RC handle_request(SQLStageEvent *sql_event);
};