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
// Created by WangYunlai on 2022/11/17.
//
// 确保头文件只被包含一次
#pragma once

#include <memory>  // 提供智能指针的支持
#include <string>  // 提供字符串类的支持

// 引入相关类的定义
#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"

// 声明Session类，表示会话
class Session;

/**
 * @brief SQL执行结果
 * @details
 * 如果当前SQL生成了执行计划，那么在返回客户端时，调用执行计划返回结果。
 * 否则返回的结果就是当前SQL的执行结果，比如DDL语句，通过return_code和state_string来描述。
 * 如果出现了一些错误，也可以通过return_code和state_string来获取信息。
 */
class SqlResult
{
public:
  // 构造函数，接收一个Session对象的指针
  SqlResult(Session *session);
  
  // 析构函数
  ~SqlResult() {}

  // 设置结果集的元组模式
  void set_tuple_schema(const TupleSchema &schema);
  
  // 设置返回代码，表示执行结果
  void set_return_code(RC rc) { return_code_ = rc; }
  
  // 设置状态字符串，提供额外的执行信息
  void set_state_string(const std::string &state_string) { state_string_ = state_string; }

  // 设置执行计划的操作符
  void set_operator(std::unique_ptr<PhysicalOperator> oper);

  // 检查是否有操作符，即是否有执行计划
  bool has_operator() const { return operator_ != nullptr; }
  
  // 获取元组模式
  const TupleSchema &tuple_schema() const { return tuple_schema_; }
  
  // 获取返回代码
  RC return_code() const { return return_code_; }
  
  // 获取状态字符串
  const std::string &state_string() const { return state_string_; }

  // 打开结果集，准备执行
  RC open();
  
  // 关闭结果集，完成执行
  RC close();
  
  // 获取下一条元组数据
  RC next_tuple(Tuple *&tuple);
  
  // 获取下一块数据
  RC next_chunk(Chunk &chunk);

private:
  // 当前所属会话的指针
  Session                          *session_ = nullptr;
  // 执行计划的操作符，可能是一个查询计划或其他类型的操作
  std::unique_ptr<PhysicalOperator> operator_;
  // 返回的表头信息，可能存在也可能不存在
  TupleSchema                       tuple_schema_;
  // 返回代码，表示执行结果
  RC                                return_code_ = RC::SUCCESS;
  // 状态字符串，提供额外的执行信息
  std::string                       state_string_;
};
