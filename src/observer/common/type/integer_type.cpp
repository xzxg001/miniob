/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"    // 引入比较器头文件
#include "common/lang/sstream.h"       // 引入字符串流头文件
#include "common/log/log.h"            // 引入日志功能
#include "common/type/integer_type.h"  // 引入整型类型定义
#include "common/value.h"              // 引入值类型定义

/**
 * @brief 比较两个整型值
 *
 * @param left 左侧值
 * @param right 右侧值
 * @return 比较结果：负数表示左侧小于右侧，0表示相等，正数表示左侧大于右侧
 */
int IntegerType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::INTS, "left type is not integer");         // 确保左侧值为整型
  ASSERT(right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS, "right type is not numeric");  // 确保右侧值为整型或浮点型

  if (right.attr_type() == AttrType::INTS) {
    // 使用整型比较
    return common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  } else if (right.attr_type() == AttrType::FLOATS) {
    // 使用浮点型比较
    float left_val  = left.get_float();   // 将左侧整型值转换为浮点型
    float right_val = right.get_float();  // 将右侧整型值转换为浮点型
    return common::compare_float((void *)&left_val, (void *)&right_val);
  }
  return INT32_MAX;  // 如果类型不匹配，返回最大整数
}

/**
 * @brief 将两个整型值相加
 *
 * @param left 左侧值
 * @param right 右侧值
 * @param result 结果值
 * @return 操作结果
 */
RC IntegerType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() + right.get_int());  // 设置结果为两者之和
  return RC::SUCCESS;                                // 返回成功
}

/**
 * @brief 将两个整型值相减
 *
 * @param left 左侧值
 * @param right 右侧值
 * @param result 结果值
 * @return 操作结果
 */
RC IntegerType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() - right.get_int());  // 设置结果为左侧减去右侧
  return RC::SUCCESS;                                // 返回成功
}

/**
 * @brief 将两个整型值相乘
 *
 * @param left 左侧值
 * @param right 右侧值
 * @param result 结果值
 * @return 操作结果
 */
RC IntegerType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_int(left.get_int() * right.get_int());  // 设置结果为两者的乘积
  return RC::SUCCESS;                                // 返回成功
}

/**
 * @brief 取整型值的负值
 *
 * @param val 输入值
 * @param result 结果值
 * @return 操作结果
 */
RC IntegerType::negative(const Value &val, Value &result) const
{
  result.set_int(-val.get_int());  // 设置结果为输入值的负值
  return RC::SUCCESS;              // 返回成功
}

/**
 * @brief 从字符串设置整型值
 *
 * @param val 结果值
 * @param data 输入字符串
 * @return 操作结果
 */
RC IntegerType::set_value_from_str(Value &val, const string &data) const
{
  RC           rc = RC::SUCCESS;    // 初始化返回值
  stringstream deserialize_stream;  // 创建字符串流用于解析
  deserialize_stream.clear();       // 清理流的状态，防止多次解析异常
  deserialize_stream.str(data);     // 设置流的内容为输入字符串

  int int_value;                    // 存储解析后的整型值
  deserialize_stream >> int_value;  // 从流中提取整型值

  // 检查解析是否成功且流是否到达末尾
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;  // 如果解析失败，设置错误码
  } else {
    val.set_int(int_value);  // 设置结果值为解析的整型值
  }
  return rc;  // 返回操作结果
}

/**
 * @brief 将整型值转换为字符串
 *
 * @param val 输入值
 * @param result 结果字符串
 * @return 操作结果
 */
RC IntegerType::to_string(const Value &val, string &result) const
{
  stringstream ss;              // 创建字符串流
  ss << val.value_.int_value_;  // 将整型值写入流
  result = ss.str();            // 将流中的内容转换为字符串
  return RC::SUCCESS;           // 返回成功
}
