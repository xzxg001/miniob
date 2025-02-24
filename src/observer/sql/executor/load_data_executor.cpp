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

// 导入必要的头文件和命名空间
#include "sql/executor/load_data_executor.h"
#include "common/lang/string.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/load_data_stmt.h"

using namespace common;

// 执行 LOAD DATA 命令
RC LoadDataExecutor::execute(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS; // 返回码初始化为成功
  SqlResult *sql_result = sql_event->session_event()->sql_result(); // 获取 SQL 结果对象
  LoadDataStmt *stmt = static_cast<LoadDataStmt *>(sql_event->stmt()); // 将语句对象转换为 LOAD DATA 语句对象
  Table *table = stmt->table(); // 获取目标表
  const char *file_name = stmt->filename(); // 获取数据文件名
  load_data(table, file_name, sql_result); // 调用 load_data 函数执行导入操作
  return rc; // 返回执行结果
}
/**
 * 从文件中导入数据时使用。尝试向表中插入解析后的一行数据。
 * @param table  要导入的表
 * @param file_values 从文件中读取到的一行数据，使用分隔符拆分后的几个字段值
 * @param record_values Table::insert_record使用的参数，为了防止频繁的申请内存
 * @param errmsg 如果出现错误，通过这个参数返回错误信息
 * @return 成功返回RC::SUCCESS
 */

// 从文件中导入数据时使用，尝试向表中插入解析后的一行数据
RC insert_record_from_file(
    Table *table, std::vector<std::string> &file_values, std::vector<Value> &record_values, std::stringstream &errmsg)
{
  const int field_num = record_values.size(); // 获取字段数量
  const int sys_field_num = table->table_meta().sys_field_num(); // 获取系统字段数量

  // 如果文件中的字段数量少于表中的字段数量，返回字段缺失错误
  if (file_values.size() < record_values.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = RC::SUCCESS; // 返回码初始化为成功

  // 遍历每个字段，将文件中的字符串值转换为相应的数据类型，并设置到 record_values 中
  for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
    const FieldMeta *field = table->table_meta().field(i + sys_field_num);
    std::string &file_value = file_values[i];
    if (field->type() != AttrType::CHARS) {
      common::strip(file_value); // 去除字符串两端的空白字符
    }
    rc = DataType::type_instance(field->type())->set_value_from_str(record_values[i], file_value); // 转换数据类型
  }

  // 如果数据类型转换成功，则尝试将记录插入表中
  if (RC::SUCCESS == rc) {
    Record record;
    rc = table->make_record(field_num, record_values.data(), record); // 创建记录
    if (rc != RC::SUCCESS) {
      errmsg << "insert failed."; // 如果创建记录失败，记录错误信息
    } else if (RC::SUCCESS != (rc = table->insert_record(record))) { // 尝试插入记录
      errmsg << "insert failed."; // 如果插入记录失败，记录错误信息
    }
  }
  return rc; // 返回操作结果
}

// 从文件中加载数据到表中
void LoadDataExecutor::load_data(Table *table, const char *file_name, SqlResult *sql_result)
{
  std::stringstream result_string; // 用于记录操作结果的字符串流

  // 打开文件
  std::fstream fs;
  fs.open(file_name, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    result_string << "Failed to open file: " << file_name << ". system error=" << strerror(errno) << std::endl;
    sql_result->set_return_code(RC::FILE_NOT_EXIST); // 设置返回码为文件不存在
    sql_result->set_state_string(result_string.str()); // 设置操作结果字符串
    return;
  }

  // 记录开始时间
  struct timespec begin_time;
  clock_gettime(CLOCK_MONOTONIC, &begin_time);
  const int sys_field_num = table->table_meta().sys_field_num(); // 获取系统字段数量
  const int field_num = table->table_meta().field_num() - sys_field_num; // 获取用户字段数量

  std::vector<Value> record_values(field_num); // 用于存储记录值的向量
  std::string line; // 用于存储文件中的每一行数据
  std::vector<std::string> file_values; // 用于存储文件中的每一行数据，分割后的字段值
  const std::string delim("|"); // 定义字段分隔符
  int line_num = 0; // 记录当前行号
  int insertion_count = 0; // 记录成功插入的记录数
  RC rc = RC::SUCCESS; // 返回码初始化为成功
  while (!fs.eof() && RC::SUCCESS == rc) {
    std::getline(fs, line); // 读取文件中的一行数据
    line_num++; // 行号加一
    if (common::is_blank(line.c_str())) {
      continue; // 如果行为空白，则跳过
    }

    file_values.clear(); // 清空文件值向量
    common::split_string(line, delim, file_values); // 根据分隔符分割行数据
    std::stringstream errmsg; // 用于记录错误信息的字符串流
    rc = insert_record_from_file(table, file_values, record_values, errmsg); // 尝试插入记录
    if (rc != RC::SUCCESS) {
      result_string << "Line:" << line_num << " insert record failed:" << errmsg.str() << ". error:" << strrc(rc)
                    << std::endl; // 如果插入记录失败，记录错误信息
    } else {
      insertion_count++; // 如果插入记录成功，插入计数加一
    }
  }
  fs.close(); // 关闭文件

  // 记录结束时间并计算操作耗时
  struct timespec end_time;
  clock_gettime(CLOCK_MONOTONIC, &end_time);
  long cost_nano = (end_time.tv_sec - begin_time.tv_sec) * 1000000000L + (end_time.tv_nsec - begin_time.tv_nsec);
  if (RC::SUCCESS == rc) {
    result_string << strrc(rc) << ". total " << line_num << " line(s) handled and " << insertion_count
                  << " record(s) loaded, total cost " << cost_nano / 1000000000.0 << " second(s)" << std::endl;
  }
  sql_result->set_return_code(RC::SUCCESS); // 设置返回码为成功
  sql_result->set_state_string(result_string.str()); // 设置操作结果字符串
}