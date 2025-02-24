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
// Created by Wangyunlai on 2022/12/07.
//

// 防止头文件重复包含的宏定义
#pragma once

#include <memory>  // 引入智能指针支持
#include <vector>  // 引入向量容器

#include "sql/expr/expression.h"  // 引入表达式相关的定义

/**
 * @brief 逻辑算子
 * @defgroup LogicalOperator
 * @details 逻辑算子描述当前执行计划要做什么，比如从表中获取数据，过滤，投影，连接等等。
 * 物理算子会描述怎么做某件事情，这是与其不同的地方。
 */

/**
 * @brief 逻辑算子类型
 *
 */
enum class LogicalOperatorType
{
  CALC,        ///< 计算算子
  TABLE_GET,   ///< 从表中获取数据
  PREDICATE,   ///< 过滤，就是谓词
  PROJECTION,  ///< 投影，就是select
  JOIN,        ///< 连接操作
  INSERT,      ///< 插入操作
  DELETE,      ///< 删除操作，删除可能会有子查询
  EXPLAIN,     ///< 查看执行计划
  GROUP_BY,    ///< 分组操作
};

/**
 * @brief 逻辑算子描述当前执行计划要做什么
 * @details 该类用于表示一个逻辑算子，包含子算子和表达式的信息。
 * 可以在优化阶段的代码中找到相关的逻辑算子操作。
 */
class LogicalOperator
{
public:
  // 默认构造函数
  LogicalOperator() = default;

  // 虚析构函数，用于子类的析构
  virtual ~LogicalOperator();

  /**
   * @brief 获取逻辑算子的类型
   * @return 返回当前逻辑算子的类型
   */
  virtual LogicalOperatorType type() const = 0;

  /**
   * @brief 添加子算子
   * @param oper 要添加的子算子
   */
  void add_child(std::unique_ptr<LogicalOperator> oper);

  /**
   * @brief 获取当前算子的所有子算子
   * @return 子算子的唯一指针向量的引用
   */
  auto children() -> std::vector<std::unique_ptr<LogicalOperator>> & { return children_; }

  /**
   * @brief 获取当前算子的所有表达式
   * @return 表达式的唯一指针向量的引用
   */
  auto expressions() -> std::vector<std::unique_ptr<Expression>> & { return expressions_; }

  /**
   * @brief 判断是否可以生成矢量化算子
   * @param type 逻辑算子类型
   * @return 如果可以生成矢量化算子则返回true，否则返回false
   */
  static bool can_generate_vectorized_operator(const LogicalOperatorType &type);

protected:
  std::vector<std::unique_ptr<LogicalOperator>> children_;  ///< 子算子的唯一指针向量

  ///< 表达式，比如select中的列，where中的谓词等等，表达式可以是常量、函数、列或子查询等
  std::vector<std::unique_ptr<Expression>> expressions_;  ///< 表达式的唯一指针向量
};
