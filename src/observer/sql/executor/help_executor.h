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
// Created by Wangyunlai on 2023/6/14.
//

// 阻止头文件被重复包含
#pragma once

// 包含必要的头文件
#include "common/rc.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/executor/sql_result.h"
#include "sql/operator/string_list_physical_operator.h"

/**
 * @brief Help语句执行器
 * @ingroup Executor
 * 这个类是Executor组的一部分，用于执行HELP命令，向用户提供可用的SQL语句示例。
 */
class HelpExecutor
{
public:
  // 默认构造函数
  HelpExecutor()          = default;
  
  // 虚析构函数，以确保派生类的正确析构
  virtual ~HelpExecutor() = default;

  // 执行方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC execute(SQLStageEvent *sql_event)
  {
    // 定义一系列SQL语句示例
    const char *strings[] = {"show tables;",
        "desc `table name`;",
        "create table `table name` (`column name` `column type`, ...);",
        "create index `index name` on `table` (`column`);",
        "insert into `table` values(`value1`,`value2`);",
        "update `table` set column=value [where `column`=`value`];",
        "delete from `table` [where `column`=`value`];",
        "select [ * | `columns` ] from `table`;"};

    // 创建一个字符串列表物理操作符，用于存储SQL语句示例
    auto oper = new StringListPhysicalOperator();
    for (size_t i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
      oper->append(strings[i]);
    }

    // 从SQL事件中获取SQL结果对象
    SqlResult *sql_result = sql_event->session_event()->sql_result();

    // 创建一个元组模式，用于存储结果集的模式
    TupleSchema schema;
    schema.append_cell("Commands");

    // 设置SQL结果的元组模式
    sql_result->set_tuple_schema(schema);
    // 设置SQL结果的操作符
    sql_result->set_operator(std::unique_ptr<PhysicalOperator>(oper));

    // 返回成功状态码
    return RC::SUCCESS;
  }
};