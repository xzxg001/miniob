/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"   // 聚合哈希表头文件
#include "sql/operator/physical_operator.h"  // 物理算子基类头文件

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  // 构造函数，接受分组表达式和聚合表达式
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
  {}  // 初始化

  virtual ~GroupByVecPhysicalOperator() = default;  // 默认析构函数

  // 返回物理算子的类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  // 打开物理算子，准备操作
  RC open(Trx *trx) override { return RC::UNIMPLEMENTED; }  // 该函数尚未实现

  // 获取下一个数据块
  RC next(Chunk &chunk) override { return RC::UNIMPLEMENTED; }  // 该函数尚未实现

  // 关闭物理算子，清理资源
  RC close() override { return RC::UNIMPLEMENTED; }  // 该函数尚未实现

private:
  // 私有成员变量可以在这里添加，例如用于存储聚合结果等
};
