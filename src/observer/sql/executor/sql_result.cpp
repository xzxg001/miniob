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
// Created by WangYunlai on 2022/11/18.
//
// 引入必要的头文件
#include "sql/executor/sql_result.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "session/session.h"
#include "storage/trx/trx.h"

// SqlResult类构造函数，接收一个Session对象的指针
SqlResult::SqlResult(Session *session) : session_(session) {}

// set_tuple_schema函数用于设置结果集的元组模式
void SqlResult::set_tuple_schema(const TupleSchema &schema) { tuple_schema_ = schema; }

// open函数用于打开结果集，准备执行SQL操作
RC SqlResult::open()
{
  // 如果没有关联的操作符，返回无效参数错误
  if (nullptr == operator_) {
    return RC::INVALID_ARGUMENT;
  }

  // 获取当前会话的事务对象
  Trx *trx = session_->current_trx();
  // 如果需要，开始事务
  trx->start_if_need();
  // 调用操作符的open方法，准备执行
  return operator_->open(trx);
}

// close函数用于关闭结果集，完成SQL操作
RC SqlResult::close()
{
  // 如果没有关联的操作符，返回无效参数错误
  if (nullptr == operator_) {
    return RC::INVALID_ARGUMENT;
  }
  // 调用操作符的close方法，完成执行
  RC rc = operator_->close();
  if (rc != RC::SUCCESS) {
    // 如果关闭操作失败，记录警告日志
    LOG_WARN("failed to close operator. rc=%s", strrc(rc));
  }

  // 重置操作符指针
  operator_.reset();

  // 如果会话不是多操作模式，根据操作结果提交或回滚事务
  if (session_ && !session_->is_trx_multi_operation_mode()) {
    if (rc == RC::SUCCESS) {
      rc = session_->current_trx()->commit();
    } else {
      RC rc2 = session_->current_trx()->rollback();
      if (rc2 != RC::SUCCESS) {
        LOG_PANIC("rollback failed. rc=%s", strrc(rc2));
      }
    }
  }
  return rc;
}

// next_tuple函数用于获取结果集中的下一条记录
RC SqlResult::next_tuple(Tuple *&tuple)
{
  // 调用操作符的next方法，获取下一条记录
  RC rc = operator_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 获取当前操作符的当前元组
  tuple = operator_->current_tuple();
  return rc;
}

// next_chunk函数用于获取结果集中的下一块数据块
RC SqlResult::next_chunk(Chunk &chunk)
{
  // 调用操作符的next方法，获取下一块数据
  RC rc = operator_->next(chunk);
  return rc;
}

// set_operator函数用于设置结果集的操作符
void SqlResult::set_operator(std::unique_ptr<PhysicalOperator> oper)
{
  // 确保当前没有其他操作符被设置
  ASSERT(operator_ == nullptr, "current operator is not null. Result is not closed?");
  // 设置操作符，并传递元组模式
  operator_ = std::move(oper);
  operator_->tuple_schema(tuple_schema_);
}