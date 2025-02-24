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
// 包含创建表执行器的头文件
#include "sql/executor/create_table_executor.h"

// 包含日志模块的头文件
#include "common/log/log.h"
// 包含会话事件的头文件
#include "event/session_event.h"
// 包含SQL事件的头文件
#include "event/sql_event.h"
// 包含会话的头文件
#include "session/session.h"
// 包含创建表语句的头文件
#include "sql/stmt/create_table_stmt.h"
// 包含数据库的头文件
#include "storage/db/db.h"

// CreateTableExecutor类用于执行创建表的SQL命令
class CreateTableExecutor
{
public:
  // 执行方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC execute(SQLStageEvent *sql_event)
  {
    // 从事件中获取SQL语句对象
    Stmt    *stmt    = sql_event->stmt();
    // 从事件中获取会话对象
    Session *session = sql_event->session_event()->session();
    // 断言当前语句类型是否为创建表，如果不是则打印错误信息
    ASSERT(stmt->type() == StmtType::CREATE_TABLE,
        "create table executor can not run this command: %d",
        static_cast<int>(stmt->type()));

    // 将Stmt对象强制转换为CreateTableStmt对象
    CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);

    // 获取要创建的表名
    const char *table_name = create_table_stmt->table_name().c_str();
    // 在当前会话的数据库中创建表，返回创建结果
    RC rc = session->get_current_db()->create_table(table_name, create_table_stmt->attr_infos(), create_table_stmt->storage_format());

    // 返回创建表的操作结果
    return rc;
  }
};