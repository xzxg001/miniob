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
// Created by Longda on 2021/4/13.
//
#include <string.h>  // 包含C标准库中的字符串处理函数
#include <string>  // 包含C++标准库中的字符串类

#include "optimize_stage.h"  // 包含优化阶段的头文件

#include "common/conf/ini.h"  // 包含配置文件解析的头文件
#include "common/io/io.h"  // 包含输入输出的头文件
#include "common/lang/string.h"  // 包含字符串操作的头文件
#include "common/log/log.h"  // 包含日志记录的头文件
#include "event/session_event.h"  // 包含会话事件的头文件
#include "event/sql_event.h"  // 包含SQL事件的头文件
#include "sql/operator/logical_operator.h"  // 包含逻辑操作符的头文件
#include "sql/stmt/stmt.h"  // 包含SQL语句的头文件

using namespace std;  // 使用标准命名空间
using namespace common;  // 使用common命名空间

// handle_request函数用于处理SQL优化阶段的请求
RC OptimizeStage::handle_request(SQLStageEvent *sql_event) {
  unique_ptr<LogicalOperator> logical_operator;  // 创建一个逻辑操作符的智能指针

  // 创建逻辑计划
  RC rc = create_logical_plan(sql_event, logical_operator);
  if (rc != RC::SUCCESS) {
    if (rc != RC::UNIMPLEMENTED) {
      LOG_WARN("failed to create logical plan. rc=%s", strrc(rc));  // 记录警告日志
    }
    return rc;  // 返回失败的返回码
  }

  ASSERT(logical_operator, "logical operator is null");  // 断言逻辑操作符不为空

  // 重写逻辑计划
  rc = rewrite(logical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite plan. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  // 优化逻辑计划
  rc = optimize(logical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to optimize plan. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  unique_ptr<PhysicalOperator> physical_operator;  // 创建一个物理操作符的智能指针
  // 生成物理计划
  rc = generate_physical_plan(logical_operator, physical_operator, sql_event->session_event()->session());
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to generate physical plan. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  sql_event->set_operator(std::move(physical_operator));  // 设置SQL事件的操作符

  return rc;  // 返回返回码
}

// optimize函数用于优化逻辑操作符，当前实现为空
RC OptimizeStage::optimize(unique_ptr<LogicalOperator> &oper) {
  // do nothing
  return RC::SUCCESS;
}

// generate_physical_plan函数用于生成物理计划
RC OptimizeStage::generate_physical_plan(
    unique_ptr<LogicalOperator> &logical_operator, unique_ptr<PhysicalOperator> &physical_operator, Session *session) {
  RC rc = RC::SUCCESS;
  // 根据执行模式和逻辑操作符类型决定使用向量化操作符还是非向量化操作符
  if (session->get_execution_mode() == ExecutionMode::CHUNK_ITERATOR && LogicalOperator::can_generate_vectorized_operator(logical_operator->type())) {
    LOG_INFO("use chunk iterator");  // 记录使用块迭代器的日志
    session->set_used_chunk_mode(true);  // 设置使用块模式
    rc    = physical_plan_generator_.create_vec(*logical_operator, physical_operator);  // 生成向量化物理计划
  } else {
    LOG_INFO("use tuple iterator");  // 记录使用元组迭代器的日志
    session->set_used_chunk_mode(false);  // 设置不使用块模式
    rc = physical_plan_generator_.create(*logical_operator, physical_operator);  // 生成非向量化物理计划
  }
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));  // 记录警告日志
  }
  return rc;  // 返回返回码
}

// rewrite函数用于重写逻辑操作符
RC OptimizeStage::rewrite(unique_ptr<LogicalOperator> &logical_operator) {
  RC rc = RC::SUCCESS;

  bool change_made = false;
  do {
    change_made = false;
    rc          = rewriter_.rewrite(logical_operator, change_made);  // 重写逻辑操作符
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to do expression rewrite on logical plan. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }
  } while (change_made);  // 如果有变化则继续重写

  return rc;  // 返回返回码
}

// create_logical_plan函数用于创建逻辑计划
RC OptimizeStage::create_logical_plan(SQLStageEvent *sql_event, unique_ptr<LogicalOperator> &logical_operator) {
  Stmt *stmt = sql_event->stmt();  // 获取SQL语句
  if (nullptr == stmt) {  // 如果SQL语句为空
    return RC::UNIMPLEMENTED;  // 返回未实现的返回码
  }

  return logical_plan_generator_.create(stmt, logical_operator);  // 创建逻辑计划
}