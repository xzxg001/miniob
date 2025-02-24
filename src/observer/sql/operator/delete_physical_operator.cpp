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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/delete_physical_operator.h"  // 包含删除物理算子的头文件
#include "common/log/log.h"                         // 包含日志记录的头文件
#include "storage/table/table.h"                    // 包含表的定义
#include "storage/trx/trx.h"                        // 包含事务的定义

/**
 * @brief 打开物理算子，准备执行删除操作
 *
 * @param trx 指向当前事务的指针
 * @return RC 返回操作结果代码
 *
 * 此函数首先检查是否存在子操作符。如果没有子操作符，则直接返回成功。
 * 如果存在子操作符，则打开子操作符，并遍历其输出的元组，将有效记录
 * 存储到内部的 records_ 向量中。最后使用事务执行删除操作。
 */
RC DeletePhysicalOperator::open(Trx *trx)
{
  // 检查子操作符是否为空
  if (children_.empty()) {
    return RC::SUCCESS;  // 如果没有子操作符，返回成功
  }

  // 获取第一个子操作符
  std::unique_ptr<PhysicalOperator> &child = children_[0];

  // 打开子操作符并检查是否成功
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));  // 日志记录失败信息
    return rc;                                                // 返回失败代码
  }

  trx_ = trx;  // 保存当前事务指针

  // 遍历子操作符的输出记录
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();  // 获取当前元组
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));  // 如果获取失败，记录警告日志
      return rc;                                               // 返回失败代码
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);  // 强制转换为 RowTuple 类型
    Record   &record    = row_tuple->record();             // 获取记录
    records_.emplace_back(std::move(record));              // 将记录移动到内部向量中
  }

  child->close();  // 关闭子操作符

  // 先收集记录再删除
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如 VacuousTrx
  for (Record &record : records_) {
    rc = trx_->delete_record(table_, record);  // 使用事务删除记录
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));  // 如果删除失败，记录警告日志
      return rc;                                          // 返回失败代码
    }
  }

  return RC::SUCCESS;  // 返回成功
}

/**
 * @brief 获取下一条记录
 *
 * @return RC 返回操作结果代码
 *
 * 此函数在删除操作中不返回实际记录，因此总是返回记录结束状态。
 */
RC DeletePhysicalOperator::next()
{
  return RC::RECORD_EOF;  // 始终返回记录结束
}

/**
 * @brief 关闭物理算子
 *
 * @return RC 返回操作结果代码
 *
 * 此函数用于关闭物理算子，释放相关资源。在这里不需要额外处理，因此直接返回成功。
 */
RC DeletePhysicalOperator::close()
{
  return RC::SUCCESS;  // 返回成功
}
