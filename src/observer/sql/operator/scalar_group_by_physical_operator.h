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
// Created by WangYunlai on 2024/06/11.
//

#pragma once

#include "sql/operator/group_by_physical_operator.h"  // 引入父类头文件

/**
 * @brief ScalarGroupByPhysicalOperator 类
 * @details 表示没有 group by 表达式的 group by 物理算子。
 * @ingroup PhysicalOperator
 */
class ScalarGroupByPhysicalOperator : public GroupByPhysicalOperator
{
public:
  /**
   * @brief 构造函数
   * @param expressions 用于分组的表达式列表
   */
  ScalarGroupByPhysicalOperator(std::vector<Expression *> &&expressions);

  virtual ~ScalarGroupByPhysicalOperator() = default;  // 默认析构函数

  /**
   * @brief 获取物理算子类型
   * @return 物理算子类型
   */
  PhysicalOperatorType type() const override { return PhysicalOperatorType::SCALAR_GROUP_BY; }

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
   * @brief 获取当前元组
   * @return 当前元组指针
   */
  Tuple *current_tuple() override;

private:
  std::unique_ptr<GroupValueType> group_value_;      // 存储分组的值
  bool                            emitted_ = false;  // 标识是否已经输出过
};
