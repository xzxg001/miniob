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
// Created by WangYunlai on 2022/6/9.
//

#pragma once  // 确保该头文件只被包含一次

#include "sql/operator/physical_operator.h"  // 包含物理算子的基类定义

class Trx;         // 前向声明，表示事务类
class DeleteStmt;  // 前向声明，表示删除语句类

/**
 * @brief 物理算子，执行删除操作
 * @ingroup PhysicalOperator
 *
 * DeletePhysicalOperator 类负责执行从数据库中删除记录的操作。它使用给定的表
 * 来执行删除语句，并处理与事务的交互。
 */
class DeletePhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief 构造函数
   *
   * @param table 指向要删除记录的表的指针
   *
   * 构造 DeletePhysicalOperator 实例并初始化表的指针。
   */
  DeletePhysicalOperator(Table *table) : table_(table) {}

  virtual ~DeletePhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取物理算子类型
   *
   * @return 返回物理算子的类型，类型为 DELETE
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::DELETE; }

  /**
   * @brief 打开物理算子
   *
   * @param trx 指向当前事务的指针
   * @return RC 返回操作结果代码
   *
   * 打开物理算子，为执行删除操作做准备，通常会在这里进行初始化工作。
   */
  RC open(Trx *trx) override;

  /**
   * @brief 获取下一条记录
   *
   * @return RC 返回操作结果代码
   *
   * 实现获取下一条需要删除的记录，并更新状态。
   */
  RC next() override;

  /**
   * @brief 关闭物理算子
   *
   * @return RC 返回操作结果代码
   *
   * 关闭物理算子，释放相关资源。
   */
  RC close() override;

  /**
   * @brief 获取当前记录元组
   *
   * @return 返回当前记录元组指针
   *
   * 该方法返回当前操作所指向的元组（记录），在删除操作中可能不需要实现。
   */
  Tuple *current_tuple() override { return nullptr; }

private:
  Table              *table_ = nullptr;  // 指向要删除记录的表的指针
  Trx                *trx_   = nullptr;  // 指向当前事务的指针
  std::vector<Record> records_;          // 存储待删除的记录集合
};
