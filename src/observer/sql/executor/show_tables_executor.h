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
// 确保头文件只被包含一次
#pragma once

// 引入必要的头文件，这些头文件提供了类所需的支持
#include "common/rc.h"  // 提供返回码RC的定义
#include "event/session_event.h"  // 提供SessionEvent的定义
#include "event/sql_event.h"  // 提供SQLStageEvent的定义
#include "session/session.h"  // 提供Session的定义
#include "sql/executor/sql_result.h"  // 提供SqlResult的定义
#include "sql/operator/string_list_physical_operator.h"  // 提供StringListPhysicalOperator的定义
#include "storage/db/db.h"  // 提供Db的定义

/**
 * @brief 显示所有表的执行器
 * @ingroup Executor
 * @note 与CreateIndex类似，不处理并发
 */
class ShowTablesExecutor
{
public:
  // 默认构造函数
  ShowTablesExecutor()          = default;
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~ShowTablesExecutor() = default;

  // execute函数用于执行显示所有表的操作
  RC execute(SQLStageEvent *sql_event)
  {
    SqlResult    *sql_result    = sql_event->session_event()->sql_result();
    SessionEvent *session_event = sql_event->session_event();

    // 从会话事件中获取当前数据库的指针
    Db *db = session_event->session()->get_current_db();

    // 存储数据库中所有表名的向量
    std::vector<std::string> all_tables;
    // 获取数据库中所有表的名称
    db->all_tables(all_tables);

    // 创建一个元组模式，用于存储结果集中的列信息
    TupleSchema tuple_schema;
    tuple_schema.append_cell(TupleCellSpec("", "Tables_in_SYS", "Tables_in_SYS"));
    // 设置SqlResult的元组模式
    sql_result->set_tuple_schema(tuple_schema);

    // 创建一个字符串列表物理操作符
    auto oper = new StringListPhysicalOperator;
    // 将所有表名添加到操作符中
    for (const std::string &s : all_tables) {
      oper->append(s);
    }

    // 将操作符设置为SqlResult的操作符
    sql_result->set_operator(std::unique_ptr<PhysicalOperator>(oper));
    return RC::SUCCESS;  // 返回成功状态码
  }
};