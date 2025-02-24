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
// Created by Wangyunlai on 2023/6/13.
//
// 阻止头文件被重复包含
#pragma once

// 包含公共返回码定义
#include "common/rc.h"

// 声明SQLStageEvent类，该类在此文件中会被使用，但具体定义在其他地方
class SQLStageEvent;

/**
 * @brief 创建表的执行器
 * @ingroup Executor
 * 这个类是Executor组的一部分，用于执行创建表的SQL命令。
 */
class CreateTableExecutor
{
public:
  // 默认构造函数
  CreateTableExecutor()          = default;
  
  // 虚析构函数，以确保派生类的正确析构
  virtual ~CreateTableExecutor() = default;

  /**
   * @brief 执行创建表的SQL命令
   * @param sql_event 指向SQLStageEvent对象的指针，包含了要执行的SQL语句和相关环境
   * @return 返回执行结果，成功则为RC::SUCCESS，失败则为相应的错误码
   * 这个方法接受一个SQLStageEvent对象，根据其中的SQL语句类型，执行创建表的操作。
   */
  RC execute(SQLStageEvent *sql_event);
};