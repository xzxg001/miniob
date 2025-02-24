/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/log/log.h"                           // 引入日志模块的头文件
#include "sql/operator/expr_vec_physical_operator.h"  // 引入当前类的头文件
#include "sql/expr/expression_tuple.h"                // 引入表达式元组的头文件
#include "sql/expr/composite_tuple.h"                 // 引入复合元组的头文件

using namespace std;     // 使用标准命名空间
using namespace common;  // 使用 common 命名空间

/**
 * @brief 构造函数
 *
 * @param expressions 要处理的表达式列表
 *
 * 通过右值引用接收表达式列表并移动到成员变量中。
 */
ExprVecPhysicalOperator::ExprVecPhysicalOperator(std::vector<Expression *> &&expressions)
{
  expressions_ = std::move(expressions);  // 初始化成员变量 expressions_
}

/**
 * @brief 打开算子，准备执行
 *
 * @param trx 当前事务
 * @return RC 返回操作结果
 */
RC ExprVecPhysicalOperator::open(Trx *trx)
{
  // 确保该算子只有一个子算子
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];                       // 获取子算子
  RC                rc    = child.open(trx);                     // 打开子算子
  if (OB_FAIL(rc)) {                                             // 检查打开是否成功
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));  // 记录错误日志
    return rc;                                                   // 返回错误码
  }
  return rc;  // 返回成功
}

/**
 * @brief 获取下一个结果并填充到 Chunk 中
 *
 * @param chunk 要填充的 Chunk 对象
 * @return RC 返回操作结果
 */
RC ExprVecPhysicalOperator::next(Chunk &chunk)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 确保该算子只有一个子算子
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];  // 获取子算子
  chunk.reset();                            // 重置输出的 Chunk
  evaled_chunk_.reset();                    // 重置计算结果的 Chunk

  // 获取子算子的下一个结果
  if (OB_SUCC(rc = child.next(chunk_))) {
    // 遍历所有表达式，并对每个表达式进行求值
    for (size_t i = 0; i < expressions_.size(); i++) {
      auto column = std::make_unique<Column>();  // 创建新的列对象
      // 从当前 Chunk 中获取当前表达式的列
      expressions_[i]->get_column(chunk_, *column);
      evaled_chunk_.add_column(std::move(column), i);  // 将列添加到 evaled_chunk_
    }
    chunk.reference(evaled_chunk_);  // 将 evaled_chunk_ 引用到输出的 Chunk
  }
  return rc;  // 返回结果
}

/**
 * @brief 关闭算子，释放资源
 *
 * @return RC 返回操作结果
 */
RC ExprVecPhysicalOperator::close()
{
  children_[0]->close();                // 关闭子算子
  LOG_INFO("close group by operator");  // 记录关闭日志
  return RC::SUCCESS;                   // 返回成功
}
