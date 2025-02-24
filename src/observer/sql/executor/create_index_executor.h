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
 * @brief 创建索引的执行器
 * @ingroup Executor
 * @note 创建索引时不能做其它操作。MiniOB当前不完善，没有对一些并发做控制，包括schema的并发。
 * 这个类是Executor组的一部分，用于执行创建索引的SQL命令。
 * 注意：在创建索引的过程中，不能执行其他操作。这是因为MiniOB在当前版本中并不完善，
 * 还没有对包括模式(schema)并发在内的一些并发操作进行控制。
 */
class CreateIndexExecutor
{
public:
  // 默认构造函数
  CreateIndexExecutor()          = default;
  
  // 虚析构函数，以确保派生类的正确析构
  virtual ~CreateIndexExecutor() = default;

  /**
   * @brief 执行创建索引的SQL命令
   * @param sql_event 指向SQLStageEvent对象的指针，包含了要执行的SQL语句和相关环境
   * @return 返回执行结果，成功则为RC::SUCCESS，失败则为相应的错误码
   * 这个方法接受一个SQLStageEvent对象，根据其中的SQL语句类型，执行创建索引的操作。
   */
  RC execute(SQLStageEvent *sql_event);
};