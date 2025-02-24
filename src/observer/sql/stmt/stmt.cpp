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
// Created by Wangyunlai on 2022/5/22.
//
#include "sql/stmt/stmt.h"  // 包含基础的SQL语句类定义
#include "common/log/log.h"  // 包含日志记录功能
#include "sql/stmt/calc_stmt.h"  // 包含计算语句类定义
#include "sql/stmt/create_index_stmt.h"  // 包含创建索引语句类定义
#include "sql/stmt/create_table_stmt.h"  // 包含创建表语句类定义
#include "sql/stmt/delete_stmt.h"  // 包含删除语句类定义
#include "sql/stmt/desc_table_stmt.h"  // 包含描述表语句类定义
#include "sql/stmt/exit_stmt.h"  // 包含退出语句类定义
#include "sql/stmt/explain_stmt.h"  // 包含解释语句类定义
#include "sql/stmt/help_stmt.h"  // 包含帮助语句类定义
#include "sql/stmt/insert_stmt.h"  // 包含插入语句类定义
#include "sql/stmt/load_data_stmt.h"  // 包含加载数据语句类定义
#include "sql/stmt/select_stmt.h"  // 包含选择语句类定义
#include "sql/stmt/set_variable_stmt.h"  // 包含设置变量语句类定义
#include "sql/stmt/show_tables_stmt.h"  // 包含显示表语句类定义
#include "sql/stmt/trx_begin_stmt.h"  // 包含事务开始语句类定义
#include "sql/stmt/trx_end_stmt.h"  // 包含事务结束语句类定义

// 判断语句类型是否为数据定义语言（DDL）类型
bool stmt_type_ddl(StmtType type) {
  switch (type) {
    case StmtType::CREATE_TABLE:  // 创建表
    case StmtType::DROP_TABLE:  // 删除表
    case StmtType::DROP_INDEX:  // 删除索引
    case StmtType::CREATE_INDEX: {  // 创建索引
      return true;  // 是DDL类型
    }
    default: {
      return false;  // 不是DDL类型
    }
  }
}

// 根据解析后的SQL语句节点创建对应的SQL语句对象
RC Stmt::create_stmt(Db *db, ParsedSqlNode &sql_node, Stmt *&stmt) {
  stmt = nullptr;  // 初始化stmt为nullptr

  // 根据sql_node的flag字段判断SQL语句类型，并创建对应的语句对象
  switch (sql_node.flag) {
    case SCF_INSERT: {  // 插入语句
      return InsertStmt::create(db, sql_node.insertion, stmt);
    }
    case SCF_DELETE: {  // 删除语句
      return DeleteStmt::create(db, sql_node.deletion, stmt);
    }
    case SCF_SELECT: {  // 查询语句
      return SelectStmt::create(db, sql_node.selection, stmt);
    }

    case SCF_EXPLAIN: {  // 解释语句
      return ExplainStmt::create(db, sql_node.explain, stmt);
    }

    case SCF_CREATE_INDEX: {  // 创建索引语句
      return CreateIndexStmt::create(db, sql_node.create_index, stmt);
    }

    case SCF_CREATE_TABLE: {  // 创建表语句
      return CreateTableStmt::create(db, sql_node.create_table, stmt);
    }

    case SCF_DESC_TABLE: {  // 描述表语句
      return DescTableStmt::create(db, sql_node.desc_table, stmt);
    }

    case SCF_HELP: {  // 帮助语句
      return HelpStmt::create(stmt);
    }

    case SCF_SHOW_TABLES: {  // 显示表语句
      return ShowTablesStmt::create(db, stmt);
    }

    case SCF_BEGIN: {  // 事务开始语句
      return TrxBeginStmt::create(stmt);
    }

    case SCF_COMMIT:
    case SCF_ROLLBACK: {  // 事务提交或回滚语句
      return TrxEndStmt::create(sql_node.flag, stmt);
    }

    case SCF_EXIT: {  // 退出语句
      return ExitStmt::create(stmt);
    }

    case SCF_SET_VARIABLE: {  // 设置变量语句
      return SetVariableStmt::create(sql_node.set_variable, stmt);
    }

    case SCF_LOAD_DATA: {  // 加载数据语句
      return LoadDataStmt::create(db, sql_node.load_data, stmt);
    }

    case SCF_CALC: {  // 计算语句
      return CalcStmt::create(sql_node.calc, stmt);
    }

    default: {
      LOG_INFO("Command::type %d doesn't need to create statement.", sql_node.flag);
    } break;  // 如果是未知的语句类型，则记录日志并返回未实现错误码
  }
  return RC::UNIMPLEMENTED;  // 返回未实现错误码
}
