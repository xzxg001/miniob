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
// 包含必要的头文件
#include <memory>

#include "sql/executor/desc_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/operator/string_list_physical_operator.h"
#include "sql/stmt/desc_table_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

// 使用标准命名空间
using namespace std;

// DescTableExecutor类用于执行描述表的SQL命令
class DescTableExecutor
{
public:
  // 执行方法接受一个SQLStageEvent对象，该对象包含了要执行的SQL语句和相关环境
  RC execute(SQLStageEvent *sql_event)
  {
    // 初始化返回码为成功
    RC rc = RC::SUCCESS;
    // 从事件中获取SQL语句对象
    Stmt *stmt = sql_event->stmt();
    // 从事件中获取会话事件对象
    SessionEvent *session_event = sql_event->session_event();
    // 从会话事件中获取会话对象
    Session *session = session_event->session();
    // 断言当前语句类型是否为描述表，如果不是则打印错误信息
    ASSERT(stmt->type() == StmtType::DESC_TABLE,
        "desc table executor can not run this command: %d",
        static_cast<int>(stmt->type()));

    // 将Stmt对象强制转换为DescTableStmt对象
    DescTableStmt *desc_table_stmt = static_cast<DescTableStmt *>(stmt);
    // 从会话事件中获取SQL结果对象
    SqlResult *sql_result = session_event->sql_result();
    // 获取要描述的表名
    const char *table_name = desc_table_stmt->table_name().c_str();

    // 从会话中获取当前数据库对象
    Db *db = session->get_current_db();
    // 在数据库中查找表对象
    Table *table = db->find_table(table_name);
    // 如果表存在
    if (table != nullptr) {
      // 创建元组模式用于存储结果
      TupleSchema tuple_schema;
      tuple_schema.append_cell(TupleCellSpec("", "Field", "Field"));
      tuple_schema.append_cell(TupleCellSpec("", "Type", "Type"));
      tuple_schema.append_cell(TupleCellSpec("", "Length", "Length"));

      // 设置SQL结果的元组模式
      sql_result->set_tuple_schema(tuple_schema);

      // 创建一个字符串列表物理操作符对象
      auto oper = new StringListPhysicalOperator;
      // 获取表的元数据
      const TableMeta &table_meta = table->table_meta();
      // 遍历表的所有字段
      for (int i = table_meta.sys_field_num(); i < table_meta.field_num(); i++) {
        // 获取字段的元数据
        const FieldMeta *field_meta = table_meta.field(i);
        // 将字段信息添加到操作符中
        oper->append({field_meta->name(), attr_type_to_string(field_meta->type()), std::to_string(field_meta->len())});
      }

      // 将操作符设置到SQL结果中
      sql_result->set_operator(unique_ptr<PhysicalOperator>(oper));
    } else {
      // 如果表不存在，设置SQL结果的错误码和错误信息
      sql_result->set_return_code(RC::SCHEMA_TABLE_NOT_EXIST);
      sql_result->set_state_string("Table not exists");
    }
    // 返回执行结果
    return rc;
  }
};