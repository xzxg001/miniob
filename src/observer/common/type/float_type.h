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

#include "common/type/data_type.h"  // 引入数据类型基类

/**
 * @brief 浮点型数据类型
 * @ingroup DataType
 */
class FloatType : public DataType
{
public:
  // 构造函数，初始化浮点型数据类型
  FloatType() : DataType(AttrType::FLOATS) {}

  // 默认析构函数
  virtual ~FloatType() = default;

  /**
   * @brief 比较两个浮点数值
   *
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果：负数表示左侧小于右侧，0表示相等，正数表示左侧大于右侧
   */
  int compare(const Value &left, const Value &right) const override;

  /**
   * @brief 将两个浮点数值相加
   *
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 操作结果
   */
  RC add(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 将两个浮点数值相减
   *
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 操作结果
   */
  RC subtract(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 将两个浮点数值相乘
   *
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 操作结果
   */
  RC multiply(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 将两个浮点数值相除
   *
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 操作结果
   */
  RC divide(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 取浮点数值的负值
   *
   * @param val 输入值
   * @param result 结果值
   * @return 操作结果
   */
  RC negative(const Value &val, Value &result) const override;

  /**
   * @brief 从字符串设置浮点数值
   *
   * @param val 结果值
   * @param data 输入字符串
   * @return 操作结果
   */
  RC set_value_from_str(Value &val, const string &data) const override;

  /**
   * @brief 将浮点数值转换为字符串
   *
   * @param val 输入值
   * @param result 结果字符串
   * @return 操作结果
   */
  RC to_string(const Value &val, string &result) const override;
};
