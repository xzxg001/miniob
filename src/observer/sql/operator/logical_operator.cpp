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
// Created by Wangyunlai on 2022/12/08.
//

#include "sql/operator/logical_operator.h"  // 引入逻辑算子的定义

// 逻辑算子的虚析构函数，负责清理资源
LogicalOperator::~LogicalOperator() {}

// 添加子逻辑算子
void LogicalOperator::add_child(std::unique_ptr<LogicalOperator> oper)
{
  // 将新的子算子移动到children_向量中
  children_.emplace_back(std::move(oper));
}

/**
 * @brief 判断是否可以生成矢量化算子
 * @param type 逻辑算子的类型
 * @return 如果可以生成矢量化算子返回true，否则返回false
 */
bool LogicalOperator::can_generate_vectorized_operator(const LogicalOperatorType &type)
{
  bool bool_ret = false;  // 初始化返回值为false
  switch (type)           // 根据传入的逻辑算子类型进行判断
  {
    case LogicalOperatorType::CALC:    // 如果是计算算子
    case LogicalOperatorType::DELETE:  // 如果是删除算子
    case LogicalOperatorType::INSERT:  // 如果是插入算子
      bool_ret = false;                // 不可以生成矢量化算子
      break;

    default:            // 对于其他类型的算子
      bool_ret = true;  // 可以生成矢量化算子
      break;
  }
  return bool_ret;  // 返回判断结果
}
