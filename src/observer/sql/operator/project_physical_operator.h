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

#pragma once  // 防止头文件被重复包含

#include "sql/operator/physical_operator.h"  // 包含物理算子类的定义
#include "sql/expr/expression_tuple.h"       // 包含表达式元组类的定义

/**
 * @brief ProjectPhysicalOperator类表示选择/投影的物理算子
 * @ingroup PhysicalOperator
 */
class ProjectPhysicalOperator : public PhysicalOperator
{
public:
  /**
   * @brief ProjectPhysicalOperator 构造函数
   * @param expressions 用于投影的表达式列表
   * @details 此构造函数将接收到的表达式移动到类的内部成员中，初始化投影物理算子。
   */
  ProjectPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions);

  virtual ~ProjectPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取物理算子的类型
   * @return 物理算子类型 PROJECT
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::PROJECT; }

  /**
   * @brief 打开物理算子并准备数据
   * @param trx 事务对象
   * @return RC 返回执行结果状态
   */
  RC open(Trx *trx) override;

  /**
   * @brief 获取下一个结果
   * @return RC 返回执行结果状态
   */
  RC next() override;

  /**
   * @brief 关闭物理算子并释放资源
   * @return RC 返回执行结果状态
   */
  RC close() override;

  /**
   * @brief 获取当前元组的单元格数量
   * @return 单元格数量
   */
  int cell_num() const { return tuple_.cell_num(); }

  /**
   * @brief 获取当前元组
   * @return 当前元组指针
   */
  Tuple *current_tuple() override;

  /**
   * @brief 获取元组模式
   * @param schema 存储元组模式的引用
   * @return RC 返回执行结果状态
   */
  RC tuple_schema(TupleSchema &schema) const override;

private:
  std::vector<std::unique_ptr<Expression>>     expressions_;  ///< 用于投影的表达式列表
  ExpressionTuple<std::unique_ptr<Expression>> tuple_;        ///< 存储结果元组的表达式元组
};
