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

#pragma once

#include "common/value.h"  // 引入 Value 类定义
#include "common/rc.h"     // 引入 RC 类型定义，用于表示操作结果状态

/**
 * @brief Aggregator 抽象基类，定义聚合操作的接口。
 *
 * 所有聚合器都应继承此类并实现其抽象方法。
 */
class Aggregator
{
public:
  virtual ~Aggregator() = default;  // 虚析构函数，确保派生类正确析构

  /**
   * @brief 将输入值累加到聚合器中。
   *
   * @param value 要累加的输入值。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   */
  virtual RC accumulate(const Value &value) = 0;

  /**
   * @brief 评估当前聚合的结果。
   *
   * @param result 输出参数，保存聚合的最终结果。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   */
  virtual RC evaluate(Value &result) = 0;

protected:
  Value value_;  // 存储聚合结果的值
};

/**
 * @brief SumAggregator 类，继承自 Aggregator，实现求和功能。
 */
class SumAggregator : public Aggregator
{
public:
  /**
   * @brief 将输入值累加到当前聚合值中。
   *
   * @param value 要累加的输入值。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   */
  RC accumulate(const Value &value) override;

  /**
   * @brief 评估当前聚合的求和结果。
   *
   * @param result 输出参数，保存聚合的最终结果。
   * @return RC 返回操作的结果代码，成功则返回 RC::SUCCESS。
   */
  RC evaluate(Value &result) override;
};
