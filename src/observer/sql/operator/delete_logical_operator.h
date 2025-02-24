/* Copyright (c) OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/12/26.
//

#pragma once  // 防止头文件被多次包含

#include "sql/operator/logical_operator.h"  // 包含逻辑操作符的基类头文件

/**
 * @brief 逻辑算子，用于执行delete语句
 *
 * @details 此类表示数据库中的删除操作。它封装了对表的删除操作，并
 * 在执行计划中指定要删除的目标表。删除逻辑算子在执行时会产生删除
 * 操作的执行计划。
 *
 * @ingroup LogicalOperator
 */
class DeleteLogicalOperator
    : public LogicalOperator  // 定义一个名为 DeleteLogicalOperator 的类，继承自 LogicalOperator 类
{
public:
  /**
   * @brief 构造函数
   *
   * @param table 指向要删除的表的指针
   *
   * @details 该构造函数用于初始化 DeleteLogicalOperator 对象，接受一个
   * 指向要删除的表的指针并将其赋值给成员变量 table_。
   */
  DeleteLogicalOperator(Table *table);  // 声明构造函数，接受一个指向 Table 的指针

  virtual ~DeleteLogicalOperator() = default;  // 默认析构函数，虚析构确保正确析构基类

  /**
   * @brief 获取逻辑操作符的类型
   *
   * @return 返回逻辑操作符类型，类型为 DELETE
   */
  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::DELETE;
  }  // 重写 type() 方法，返回逻辑操作符类型为 DELETE

  /**
   * @brief 获取要删除的表
   *
   * @return 返回指向目标表的指针
   */
  Table *table() const { return table_; }  // 返回指向要删除的表的指针

private:
  Table *table_ = nullptr;  // 指向要删除的表的指针，初始化为 nullptr
};
