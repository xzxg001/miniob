/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once  // 防止头文件重复包含

#include "sql/operator/physical_operator.h"  // 引入物理算子基类
#include "sql/expr/expression_tuple.h"       // 引入表达式元组相关功能

/**
 * @brief 选择/投影物理算子（向量化）
 * @ingroup PhysicalOperator
 * @details 此类实现了向量化的投影物理算子，支持高效的数据处理。
 */
class ProjectVecPhysicalOperator : public PhysicalOperator
{
public:
  ProjectVecPhysicalOperator() {}  // 默认构造函数

  /**
   * @brief 带表达式的构造函数
   * @param expressions 用于投影的表达式列表
   * @details 此构造函数初始化投影物理算子，接收表达式并存储。
   */
  ProjectVecPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions);

  virtual ~ProjectVecPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取算子类型
   * @return 返回物理算子类型
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::PROJECT_VEC; }

  /**
   * @brief 打开物理算子并准备数据
   * @param trx 事务对象
   * @return RC 返回执行结果状态
   */
  RC open(Trx *trx) override;

  /**
   * @brief 获取下一个结果
   * @param chunk 用于存储结果的块
   * @return RC 返回执行结果状态
   */
  RC next(Chunk &chunk) override;

  /**
   * @brief 关闭物理算子并释放资源
   * @return RC 返回执行结果状态
   */
  RC close() override;

  /**
   * @brief 获取元组模式
   * @param schema 存储元组模式的引用
   * @return RC 返回执行结果状态
   */
  RC tuple_schema(TupleSchema &schema) const override;

  /**
   * @brief 获取表达式列表
   * @return 返回表达式列表的引用
   */
  std::vector<std::unique_ptr<Expression>> &expressions() { return expressions_; }

private:
  std::vector<std::unique_ptr<Expression>> expressions_;  // 存储用于投影的表达式
  Chunk                                    chunk_;        // 存储结果的块
};
