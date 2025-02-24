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

#include "sql/operator/predicate_physical_operator.h"  // 引入PredicatePhysicalOperator的定义
#include "common/log/log.h"                            // 引入日志功能
#include "sql/stmt/filter_stmt.h"                      // 引入过滤语句的定义
#include "storage/field/field.h"                       // 引入字段相关的定义
#include "storage/record/record.h"                     // 引入记录相关的定义

/**
 * @brief 构造函数，初始化PredicatePhysicalOperator对象
 * @param expr 用于设置的表达式，表示过滤条件
 */
PredicatePhysicalOperator::PredicatePhysicalOperator(std::unique_ptr<Expression> expr)
    : expression_(std::move(expr))  // 移动传入的表达式到成员变量
{
  // 确保表达式的值类型为布尔型
  ASSERT(expression_->value_type() == AttrType::BOOLEANS, "predicate's expression should be BOOLEAN type");
}

/**
 * @brief 打开物理算子，进行初始化
 * @param trx 事务指针，用于执行上下文
 * @return 返回执行结果代码
 */
RC PredicatePhysicalOperator::open(Trx *trx)
{
  // 检查子算子的数量，必须有一个子算子
  if (children_.size() != 1) {
    LOG_WARN("predicate operator must has one child");  // 记录警告日志
    return RC::INTERNAL;                                // 返回内部错误
  }

  // 打开子算子并返回结果
  return children_[0]->open(trx);
}

/**
 * @brief 获取下一个元组
 * @return 返回执行结果代码
 */
RC PredicatePhysicalOperator::next()
{
  RC                rc   = RC::SUCCESS;              // 初始化返回代码为成功
  PhysicalOperator *oper = children_.front().get();  // 获取第一个子算子

  // 循环获取子算子的下一个元组
  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();             // 获取当前元组
    if (nullptr == tuple) {                           // 检查元组是否有效
      rc = RC::INTERNAL;                              // 返回内部错误
      LOG_WARN("failed to get tuple from operator");  // 记录警告日志
      break;                                          // 退出循环
    }

    Value value;                                 // 创建一个值对象
    rc = expression_->get_value(*tuple, value);  // 获取当前元组在表达式下的值
    if (rc != RC::SUCCESS) {                     // 检查获取值的结果
      return rc;                                 // 返回错误代码
    }

    // 如果值为真，返回成功
    if (value.get_boolean()) {
      return rc;
    }
  }
  return rc;  // 返回当前结果代码
}

/**
 * @brief 关闭物理算子，进行资源释放
 * @return 返回执行结果代码
 */
RC PredicatePhysicalOperator::close()
{
  // 关闭子算子并返回成功
  children_[0]->close();
  return RC::SUCCESS;
}

/**
 * @brief 获取当前元组
 * @return 返回当前元组的指针
 */
Tuple *PredicatePhysicalOperator::current_tuple()
{
  return children_[0]->current_tuple();  // 获取子算子的当前元组
}

/**
 * @brief 获取元组的模式
 * @param schema 输出参数，用于保存元组的模式
 * @return 返回执行结果代码
 */
RC PredicatePhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  // 获取子算子的元组模式并返回结果
  return children_[0]->tuple_schema(schema);
}
