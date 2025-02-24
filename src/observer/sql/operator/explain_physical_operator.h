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
// Created by WangYunlai on 2022/12/27.
//

#pragma once  // 确保该头文件仅被包含一次

#include "sql/operator/physical_operator.h"  // 引入物理算子的头文件

/**
 * @brief Explain物理算子
 *
 * 该类表示用于执行解释操作的物理算子。它继承自
 * PhysicalOperator 基类，并实现特定的物理算子类型。
 * @ingroup PhysicalOperator
 */
class ExplainPhysicalOperator : public PhysicalOperator
{
public:
  ExplainPhysicalOperator()          = default;  // 默认构造函数
  virtual ~ExplainPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取物理算子的类型
   *
   * @return PhysicalOperatorType 返回当前物理算子的类型
   *
   * 此方法重载自基类，返回解释算子的物理类型。
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::EXPLAIN; }  // 返回物理算子类型为 EXPLAIN

  RC     open(Trx *trx) override;      // 打开算子，准备执行
  RC     next() override;              // 获取下一个结果
  RC     next(Chunk &chunk) override;  // 获取下一个结果并填充到 Chunk 中
  RC     close() override;             // 关闭算子，释放资源
  Tuple *current_tuple() override;     // 获取当前元组

  /**
   * @brief 获取元组的模式
   *
   * @param schema 用于存储元组模式的结构
   * @return RC 返回操作的状态
   *
   * 此方法定义元组的结构，添加“Query Plan”作为列名。
   */
  RC tuple_schema(TupleSchema &schema) const override
  {
    schema.append_cell("Query Plan");  // 在模式中添加名为 "Query Plan" 的列
    return RC::SUCCESS;                // 返回成功状态
  }

private:
  /**
   * @brief 将物理算子转换为字符串
   *
   * @param os 输出流，用于输出算子的字符串表示
   * @param oper 当前物理算子
   * @param level 当前算子在层级中的位置
   * @param last_child 当前算子是否为该层级中的最后一个子节点
   * @param ends 记录当前层级是否还有其他节点的布尔向量
   */
  void to_string(std::ostream &os, PhysicalOperator *oper, int level, bool last_child, std::vector<bool> &ends);

  /**
   * @brief 生成物理执行计划
   *
   * 此方法负责生成当前物理算子的执行计划。
   */
  void generate_physical_plan();

private:
  std::string    physical_plan_;  // 存储生成的物理执行计划字符串
  ValueListTuple tuple_;          // 当前元组，用于存储值的列表
};
