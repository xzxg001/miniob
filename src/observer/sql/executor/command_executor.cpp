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
// 包含执行SQL命令所需的头文件
#include "sql/executor/command_executor.h"
#include "common/log/log.h"
#include "event/sql_event.h"
#include "sql/executor/create_index_executor.h"
#include "sql/executor/create_table_executor.h"
#include "sql/executor/desc_table_executor.h"
#include "sql/executor/help_executor.h"
#include "sql/executor/load_data_executor.h"
#include "sql/executor/set_variable_executor.h"
#include "sql/executor/show_tables_executor.h"
#include "sql/executor/trx_begin_executor.h"
#include "sql/executor/trx_end_executor.h"
#include "sql/stmt/stmt.h"

// CommandExecutor类用于执行SQL命令
class CommandExecutor {
public:
  // execute方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC execute(SQLStageEvent *sql_event) {
    // 从事件中获取SQL语句对象
    Stmt *stmt = sql_event->stmt();

    // 初始化返回码为成功
    RC rc = RC::SUCCESS;
    // 根据SQL语句的类型执行不同的操作
    switch (stmt->type()) {
      // 如果是创建索引的语句，使用CreateIndexExecutor执行
      case StmtType::CREATE_INDEX: {
        CreateIndexExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是创建表的语句，使用CreateTableExecutor执行
      case StmtType::CREATE_TABLE: {
        CreateTableExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是描述表的语句，使用DescTableExecutor执行
      case StmtType::DESC_TABLE: {
        DescTableExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是帮助命令，使用HelpExecutor执行
      case StmtType::HELP: {
        HelpExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是显示表的命令，使用ShowTablesExecutor执行
      case StmtType::SHOW_TABLES: {
        ShowTablesExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是开始事务的命令，使用TrxBeginExecutor执行
      case StmtType::BEGIN: {
        TrxBeginExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是提交或回滚事务的命令，使用TrxEndExecutor执行
      case StmtType::COMMIT:
      case StmtType::ROLLBACK: {
        TrxEndExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是设置变量的命令，使用SetVariableExecutor执行
      case StmtType::SET_VARIABLE: {
        SetVariableExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是加载数据的命令，使用LoadDataExecutor执行
      case StmtType::LOAD_DATA: {
        LoadDataExecutor executor;
        rc = executor.execute(sql_event);
      } break;

      // 如果是退出命令，返回成功
      case StmtType::EXIT: {
        rc = RC::SUCCESS;
      } break;

      // 如果遇到未知的命令，记录错误并返回未实现的返回码
      default: {
        LOG_ERROR("unknown command: %d", static_cast<int>(stmt->type()));
        rc = RC::UNIMPLEMENTED;
      } break;
    }

    // 如果执行成功并且是DDL（数据定义语言）命令，则同步数据库以确保元数据与日志一致
    if (OB_SUCC(rc) && stmt_type_ddl(stmt->type())) {
      rc = sql_event->session_event()->session()->get_current_db()->sync();
      LOG_INFO("sync db after ddl. rc=%d", rc);
    }

    // 返回执行结果
    return rc;
  }
};