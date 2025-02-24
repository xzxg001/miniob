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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

/**
 * @brief SumAggregator 类，用于实现聚合操作中的求和功能。
 */
class SumAggregator
{
public:
  // 当前聚合的值
  Value value_;

  /**
   * @brief 累加输入的值到当前聚合值中。
   *
   * @param value 输入的值，将被累加到当前聚合值中。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   *
   * @details
   * 如果当前聚合值未定义（即初始状态），将输入值直接赋值给聚合值。
   * 如果当前聚合值已经定义，则检查输入值的类型与聚合值类型是否一致，
   * 若一致则将输入值累加到当前聚合值中。
   *
   * @assert
   * 使用 ASSERT 检查输入值与当前聚合值类型一致，如果不一致，抛出异常。
   */
  RC accumulate(const Value &value);

  /**
   * @brief 评估当前的聚合结果。
   *
   * @param result 输出参数，保存聚合的最终结果。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   *
   * @details
   * 将当前的聚合值赋值给结果参数。
   */
  RC evaluate(Value &result);
};

RC SumAggregator::accumulate(const Value &value)
{
  // 如果聚合值未定义，直接赋值
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  // 确保输入值与聚合值类型一致
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  // 执行累加操作
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value &result)
{
  // 将聚合结果保存到输出参数中
  result = value_;
  return RC::SUCCESS;
}
