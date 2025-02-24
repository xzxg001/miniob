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
// Created by Wangyunlai on 2023/7/12.
//
// 引入必要的头文件
#pragma once  // 确保头文件只被包含一次
#include "common/rc.h"  // 包含RC类，用于返回状态码

// 引入相关类，这些类在代码中被使用，但未在代码片段中定义
class SQLStageEvent;
class Table;
class SqlResult;

/**
 * @brief 导入数据的执行器
 * @ingroup Executor
 */
class LoadDataExecutor
{
public:
  // 默认构造函数
  LoadDataExecutor()          = default;
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~LoadDataExecutor() = default;

  // execute函数用于执行导入数据的操作
  // 它接收一个SQLStageEvent类型的指针，表示与SQL相关的事件
  RC execute(SQLStageEvent *sql_event);

private:
  // load_data函数用于将数据从文件加载到表中
  // table参数表示要导入数据的表
  // file_name参数表示包含要导入数据的文件的名称
  // sql_result参数表示SQL操作的结果
  void load_data(Table *table, const char *file_name, SqlResult *sql_result);
};
