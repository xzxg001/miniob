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

#pragma once  // 防止头文件重复包含

#include "sql/expr/expression.h"             // 引入表达式相关的定义
#include "sql/operator/physical_operator.h"  // 引入物理算子相关的定义

class FilterStmt;  // 前向声明FilterStmt类，用于过滤语句的表示

/**
 * @brief 过滤/谓词物理算子
 * @ingroup PhysicalOperator
 * @details 该类表示物理执行计划中的过滤操作（谓词），用于在执行计划中应用条件。
 */
class PredicatePhysicalOperator : public PhysicalOperator  // 继承自PhysicalOperator类
{
public:
  /**
   * @brief 构造函数，初始化PredicatePhysicalOperator对象
   * @param expr 用于设置的表达式，表示过滤条件
   */
  PredicatePhysicalOperator(std::unique_ptr<Expression> expr);

  // 默认析构函数
  virtual ~PredicatePhysicalOperator() = default;

  /**
   * @brief 获取物理算子的类型
   * @return 返回物理算子的类型，具体为PREDICATE
   */
  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::PREDICATE;  // 返回PREDICATE类型
  }

  /**
   * @brief 打开物理算子，进行初始化
   * @param trx 事务指针，用于执行上下文
   * @return 返回执行结果代码
   */
  RC open(Trx *trx) override;

  /**
   * @brief 获取下一个元组
   * @return 返回执行结果代码
   */
  RC next() override;

  /**
   * @brief 关闭物理算子，进行资源释放
   * @return 返回执行结果代码
   */
  RC close() override;

  /**
   * @brief 获取当前元组
   * @return 返回当前元组的指针
   */
  Tuple *current_tuple() override;

  /**
   * @brief 获取元组的模式
   * @param schema 输出参数，用于保存元组的模式
   * @return 返回执行结果代码
   */
  RC tuple_schema(TupleSchema &schema) const override;

private:
  std::unique_ptr<Expression> expression_;  // 存储过滤条件的表达式
};
