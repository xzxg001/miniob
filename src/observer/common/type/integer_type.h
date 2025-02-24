/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once  // 确保该头文件只被包含一次

#include "common/type/data_type.h"  // 引入基础数据类型的定义

/**
 * @brief 整型类型
 * @ingroup DataType
 *
 * IntegerType 类表示整型数据类型，继承自 DataType。
 * 提供了整型的比较、算术运算、字符串转换等功能。
 */
class IntegerType : public DataType
{
public:
  // 构造函数，初始化为整型属性类型
  IntegerType() : DataType(AttrType::INTS) {}

  // 析构函数
  virtual ~IntegerType() {}

  /**
   * @brief 比较两个值
   * @param left 左侧值
   * @param right 右侧值
   * @return 比较结果
   */
  int compare(const Value &left, const Value &right) const override;

  /**
   * @brief 将两个整型值相加
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 返回操作结果
   */
  RC add(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 将两个整型值相减
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 返回操作结果
   */
  RC subtract(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 将两个整型值相乘
   * @param left 左侧值
   * @param right 右侧值
   * @param result 结果值
   * @return 返回操作结果
   */
  RC multiply(const Value &left, const Value &right, Value &result) const override;

  /**
   * @brief 取整型值的负值
   * @param val 输入值
   * @param result 结果值
   * @return 返回操作结果
   */
  RC negative(const Value &val, Value &result) const override;

  /**
   * @brief 从字符串设置整型值
   * @param val 结果值
   * @param data 输入字符串
   * @return 返回操作结果
   */
  RC set_value_from_str(Value &val, const string &data) const override;

  /**
   * @brief 将整型值转换为字符串
   * @param val 输入值
   * @param result 结果字符串
   * @return 返回操作结果
   */
  RC to_string(const Value &val, string &result) const override;
};
