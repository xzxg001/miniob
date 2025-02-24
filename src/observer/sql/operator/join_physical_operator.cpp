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
// Created by WangYunlai on 2022/12/30
//

#include "sql/operator/join_physical_operator.h"

// 构造函数
NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator() {}

// 打开连接操作，初始化左右表
RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;  // 如果子节点不是两个，返回错误
  }

  RC rc         = RC::SUCCESS;         // 初始化返回代码为成功
  left_         = children_[0].get();  // 获取左表的物理算子
  right_        = children_[1].get();  // 获取右表的物理算子
  right_closed_ = true;                // 右表初始状态为关闭
  round_done_   = true;                // 初始标志位，表示右表是否完成一轮遍历

  rc   = left_->open(trx);  // 打开左表
  trx_ = trx;               // 保存当前事务
  return rc;                // 返回状态码
}

// 获取下一个结果元组
RC NestedLoopJoinPhysicalOperator::next()
{
  bool left_need_step = (left_tuple_ == nullptr);  // 左元组是否需要更新
  RC   rc             = RC::SUCCESS;               // 初始化返回代码为成功
  if (round_done_) {
    left_need_step = true;  // 如果右表一轮结束，左元组需要更新
  } else {
    rc = right_next();  // 尝试从右表获取下一个元组
    if (rc != RC::SUCCESS) {
      if (rc == RC::RECORD_EOF) {
        left_need_step = true;  // 右表遍历完一轮，左元组需要更新
      } else {
        return rc;  // 其他错误返回
      }
    } else {
      return rc;  // 成功从右表获取元组
    }
  }

  // 如果左元组需要更新，则获取下一个左元组
  if (left_need_step) {
    rc = left_next();  // 获取左表的下一个元组
    if (rc != RC::SUCCESS) {
      return rc;  // 如果失败，返回错误
    }
  }

  rc = right_next();  // 获取右表的下一个元组
  return rc;          // 返回状态码
}

// 关闭连接操作，释放相关资源
RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();  // 关闭左表
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));  // 关闭失败，记录警告
  }

  if (!right_closed_) {
    rc = right_->close();  // 关闭右表
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close right oper. rc=%s", strrc(rc));  // 关闭失败，记录警告
    } else {
      right_closed_ = true;  // 关闭成功，更新状态
    }
  }
  return rc;  // 返回状态码
}

// 获取当前关联的元组
Tuple *NestedLoopJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

// 获取下一个左元组
RC NestedLoopJoinPhysicalOperator::left_next()
{
  RC rc = RC::SUCCESS;    // 初始化返回代码为成功
  rc    = left_->next();  // 获取左表的下一个元组
  if (rc != RC::SUCCESS) {
    return rc;  // 如果失败，返回错误
  }

  left_tuple_ = left_->current_tuple();  // 更新当前左元组
  joined_tuple_.set_left(left_tuple_);   // 设置当前关联的左元组
  return rc;                             // 返回状态码
}

// 获取下一个右元组
RC NestedLoopJoinPhysicalOperator::right_next()
{
  RC rc = RC::SUCCESS;  // 初始化返回代码为成功
  if (round_done_) {    // 如果一轮右表遍历完成
    if (!right_closed_) {
      rc            = right_->close();  // 关闭右表
      right_closed_ = true;             // 更新状态
      if (rc != RC::SUCCESS) {
        return rc;  // 如果关闭失败，返回错误
      }
    }

    rc = right_->open(trx_);  // 打开右表
    if (rc != RC::SUCCESS) {
      return rc;  // 如果打开失败，返回错误
    }
    right_closed_ = false;  // 更新状态
    round_done_   = false;  // 重置标志位
  }

  rc = right_->next();  // 获取右表的下一个元组
  if (rc != RC::SUCCESS) {
    if (rc == RC::RECORD_EOF) {
      round_done_ = true;  // 如果右表遍历结束，更新状态
    }
    return rc;  // 返回状态码
  }

  right_tuple_ = right_->current_tuple();  // 更新当前右元组
  joined_tuple_.set_right(right_tuple_);   // 设置当前关联的右元组
  return rc;                               // 返回状态码
}
