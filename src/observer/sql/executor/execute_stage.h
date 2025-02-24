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
// 阻止头文件被重复包含
#pragma once

// 包含公共返回码定义
#include "common/rc.h"

// 声明SQLStageEvent类，该类在此文件中会被使用，但具体定义在其他地方
class SQLStageEvent;
class SessionEvent;
class SelectStmt;

/**
 * @brief 执行SQL语句的Stage，包括DML和DDL
 * @ingroup SQLStage
 * @details 根据前面阶段生成的结果，有些语句会生成执行计划，有些不会。
 * 整体上分为两类，带执行计划的，或者 CommandExecutor 可以直接执行的。
 * 这个类是SQL执行阶段的一部分，用于处理SQL语句的执行。
 * 它处理两种类型的SQL语句：那些生成执行计划的语句和那些CommandExecutor可以直接执行的语句。
 */
class ExecuteStage
{
public:
  // handle_request方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  // 它根据事件类型来决定如何执行SQL语句
  RC handle_request(SQLStageEvent *event);

  // handle_request_with_physical_operator方法接受一个SQLStageEvent对象
  // 这个对象包含了一个物理操作符，该操作符定义了如何执行SQL语句
  // 这个方法用于处理那些带有物理操作符的SQL语句
  RC handle_request_with_physical_operator(SQLStageEvent *sql_event);
};