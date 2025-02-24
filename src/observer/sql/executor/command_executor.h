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
// Created by Wangyunlai on 2023/4/25.
//
// 阻止头文件被重复包含
#pragma once

// 包含公共返回码定义
#include "common/rc.h"

// 声明SQLStageEvent类，该类在此文件中会被使用，但具体定义在其他地方
class SQLStageEvent;

/**
 * @defgroup Executor
 * @brief 一些SQL语句不会生成对应的执行计划，直接使用Executor来执行，比如DDL语句
 * 这个组（defgroup）是Doxygen文档生成工具的一个指令，用于创建文档中的组。
 */

/**
 * @brief 执行器
 * @ingroup Executor
 * 这个类是Executor组的一部分，用于执行SQL命令。
 */
class CommandExecutor
{
public:
  // 默认构造函数
  CommandExecutor()          = default;
  
  // 虚析构函数，以确保派生类的正确析构
  virtual ~CommandExecutor() = default;

  /**
   * @brief 执行SQL事件
   * @param sql_event 指向SQLStageEvent对象的指针，包含了要执行的SQL语句和相关环境
   * @return 返回执行结果，成功则为RC::SUCCESS，失败则为相应的错误码
   * 这个方法接受一个SQLStageEvent对象，根据其中的SQL语句类型，执行相应的操作。
   */
  RC execute(SQLStageEvent *sql_event);
};
