/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/project_vec_physical_operator.h"  // 引入对应的头文件
#include "common/log/log.h"                              // 引入日志功能
#include "storage/record/record.h"                       // 引入记录相关功能
#include "storage/table/table.h"                         // 引入表相关功能

using namespace std;  // 使用标准命名空间

/**
 * @brief ProjectVecPhysicalOperator 构造函数
 * @param expressions 用于投影的表达式列表
 * @details 初始化投影物理算子，接收并存储表达式，同时为每个表达式添加对应的列到 chunk 中。
 */
ProjectVecPhysicalOperator::ProjectVecPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
    : expressions_(std::move(expressions))  // 移动表达式以避免不必要的拷贝
{
  int expr_pos = 0;  // 表达式位置初始化
  // 遍历每个表达式，添加到 chunk 中
  for (auto &expr : expressions_) {
    chunk_.add_column(make_unique<Column>(expr->value_type(), expr->value_length()), expr_pos);
    expr_pos++;  // 更新表达式位置
  }
}

/**
 * @brief 打开物理算子并准备数据
 * @param trx 事务对象
 * @return RC 返回执行结果状态
 */
RC ProjectVecPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {  // 如果没有子算子，直接返回成功
    return RC::SUCCESS;
  }
  // 打开第一个子算子
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));  // 日志记录警告
    return rc;                                                // 返回错误状态
  }

  return RC::SUCCESS;  // 返回成功状态
}

/**
 * @brief 获取下一个结果
 * @param chunk 用于存储结果的块
 * @return RC 返回执行结果状态
 */
RC ProjectVecPhysicalOperator::next(Chunk &chunk)
{
  if (children_.empty()) {  // 如果没有子算子，返回文件结束状态
    return RC::RECORD_EOF;
  }

  chunk_.reset_data();  // 重置当前块数据
  // 获取下一个元组
  RC rc = children_[0]->next(chunk_);
  if (rc == RC::RECORD_EOF) {
    return rc;  // 返回文件结束状态
  } else if (rc == RC::SUCCESS) {
    // 将获取到的 chunk 引用给参数 chunk
    rc = chunk.reference(chunk_);
  } else {
    LOG_WARN("failed to get next tuple: %s", strrc(rc));  // 日志记录警告
    return rc;                                           // 返回错误状态
  }

  return rc;  // 返回执行结果状态
}

/**
 * @brief 关闭物理算子并释放资源
 * @return RC 返回执行结果状态
 */
RC ProjectVecPhysicalOperator::close()
{
  if (!children_.empty()) {  // 如果有子算子，关闭它
    children_[0]->close();
  }
  return RC::SUCCESS;  // 返回成功状态
}

/**
 * @brief 获取元组模式
 * @param schema 存储元组模式的引用
 * @return RC 返回执行结果状态
 */
RC ProjectVecPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  // 遍历表达式并将每个表达式的名称添加到模式中
  for (const unique_ptr<Expression> &expression : expressions_) {
    schema.append_cell(expression->name());
  }
  return RC::SUCCESS;  // 返回成功状态
}
