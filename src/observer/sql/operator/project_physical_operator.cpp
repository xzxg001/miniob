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
// Created by WangYunlai on 2022/07/01.
//

#include "sql/operator/project_physical_operator.h"  // 引入投影物理算子的头文件
#include "common/log/log.h"                          // 引入日志记录功能
#include "storage/record/record.h"                   // 引入记录相关功能
#include "storage/table/table.h"                     // 引入表相关功能

using namespace std;  // 使用标准命名空间

/**
 * @brief ProjectPhysicalOperator 构造函数
 * @param expressions 用于投影的表达式列表
 * @details 此构造函数将接收到的表达式移动到类的内部成员中，初始化投影物理算子。
 */
ProjectPhysicalOperator::ProjectPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
    : expressions_(std::move(expressions)), tuple_(expressions_)  // 移动构造表达式并初始化元组
{}

/**
 * @brief 打开物理算子并准备数据
 * @param trx 事务对象
 * @return RC 返回执行结果状态
 */
RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {  // 如果没有子算子
    return RC::SUCCESS;     // 返回成功
  }

  PhysicalOperator *child = children_[0].get();               // 获取第一个子算子
  RC                rc    = child->open(trx);                 // 打开子算子
  if (rc != RC::SUCCESS) {                                    // 如果打开失败
    LOG_WARN("failed to open child operator: %s", strrc(rc));  // 记录警告日志
    return rc;                                                // 返回错误码
  }

  return RC::SUCCESS;  // 返回成功
}

/**
 * @brief 获取下一个结果
 * @return RC 返回执行结果状态
 */
RC ProjectPhysicalOperator::next()
{
  if (children_.empty()) {  // 如果没有子算子
    return RC::RECORD_EOF;  // 返回文件结束标志
  }
  return children_[0]->next();  // 获取第一个子算子的下一个结果
}

/**
 * @brief 关闭物理算子并释放资源
 * @return RC 返回执行结果状态
 */
RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {  // 如果有子算子
    children_[0]->close();   // 关闭第一个子算子
  }
  return RC::SUCCESS;  // 返回成功
}

/**
 * @brief 获取当前元组
 * @return 当前元组指针
 */
Tuple *ProjectPhysicalOperator::current_tuple()
{
  tuple_.set_tuple(children_[0]->current_tuple());  // 设置当前元组
  return &tuple_;                                   // 返回当前元组的地址
}

/**
 * @brief 获取元组模式
 * @param schema 存储元组模式的引用
 * @return RC 返回执行结果状态
 */
RC ProjectPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  for (const unique_ptr<Expression> &expression : expressions_) {  // 遍历所有表达式
    schema.append_cell(expression->name());                        // 将表达式名称添加到模式中
  }
  return RC::SUCCESS;  // 返回成功
}
