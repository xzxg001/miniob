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
// 包含创建索引执行器的头文件
#include "sql/executor/create_index_executor.h"
// 包含日志模块的头文件
#include "common/log/log.h"
// 包含会话事件的头文件
#include "event/session_event.h"
// 包含SQL事件的头文件
#include "event/sql_event.h"
// 包含会话的头文件
#include "session/session.h"
// 包含创建索引语句的头文件
#include "sql/stmt/create_index_stmt.h"
// 包含表的头文件
#include "storage/table/table.h"

// CreateIndexExecutor类用于执行创建索引的SQL命令
class CreateIndexExecutor
{
public:
  // 执行方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC execute(SQLStageEvent *sql_event)
  {
    // 从事件中获取SQL语句对象
    Stmt    *stmt    = sql_event->stmt();
    // 从事件中获取会话对象
    Session *session = sql_event->session_event()->session();
    // 断言当前语句类型是否为创建索引，如果不是则打印错误信息
    ASSERT(stmt->type() == StmtType::CREATE_INDEX,
        "create index executor can not run this command: %d",
        static_cast<int>(stmt->type()));

    // 将Stmt对象强制转换为CreateIndexStmt对象
    CreateIndexStmt *create_index_stmt = static_cast<CreateIndexStmt *>(stmt);

    // 从会话中获取当前事务对象
    Trx   *trx   = session->current_trx();
    // 从创建索引语句中获取表对象
    Table *table = create_index_stmt->table();
    // 调用表对象的create_index方法来创建索引，并返回结果
    return table->create_index(trx, create_index_stmt->field_meta(), create_index_stmt->index_name().c_str());
  }
};