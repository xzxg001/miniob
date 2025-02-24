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
// Created by Meiyi
//
#include "sql/parser/parse.h"  // 包含SQL解析头文件
#include "common/log/log.h"    // 包含日志记录功能
#include "sql/expr/expression.h" // 包含SQL表达式处理

// 声明parse函数，用于解析SQL语句并返回解析结果
RC parse(char *st, ParsedSqlNode *sqln);

// ParsedSqlNode类的构造函数
// 默认构造函数，将标志位初始化为SCF_ERROR
ParsedSqlNode::ParsedSqlNode() : flag(SCF_ERROR) {}

// ParsedSqlNode类的构造函数
// 接收一个SqlCommandFlag类型的参数，并初始化标志位
ParsedSqlNode::ParsedSqlNode(SqlCommandFlag _flag) : flag(_flag) {}

// ParsedSqlResult类的方法，用于添加一个新的SQL节点
void ParsedSqlResult::add_sql_node(std::unique_ptr<ParsedSqlNode> sql_node)
{
  sql_nodes_.emplace_back(std::move(sql_node)); // 将unique_ptr<ParsedSqlNode>对象移动到vector中
}

////////////////////////////////////////////////////////////////////////////////

// 声明sql_parse函数，用于实际的SQL解析工作
int sql_parse(const char *st, ParsedSqlResult *sql_result);

// 实现parse函数，它调用sql_parse函数并返回操作结果
RC parse(const char *st, ParsedSqlResult *sql_result)
{
  sql_parse(st, sql_result); // 调用sql_parse函数进行解析
  return RC::SUCCESS;        // 返回成功状态码
}
