/*
版权 (c) 2021 OceanBase 及其附属机构。保留所有权利。
miniob 依据 Mulan PSL v2 许可。
您可以根据 Mulan PSL v2 的条款和条件使用本软件。
您可以在以下网址获取 Mulan PSL v2 的副本：
         http://license.coscl.org.cn/MulanPSL2
本软件是按“原样”提供的，不提供任何类型的保证，
无论是明示还是暗示，包括但不限于不侵权、
适销性或特定用途的适用性。
有关详细信息，请参阅 Mulan PSL v2。
*/

#include "common/lang/comparator.h"  // 引入比较器相关的头文件
#include "common/log/log.h"          // 引入日志相关的头文件
#include "common/type/char_type.h"   // 引入字符类型相关的头文件
#include "common/value.h"            // 引入值相关的头文件

// CharType 类的方法实现

/**
 * @brief 比较两个字符类型的值
 *
 * @param left 左侧的字符值
 * @param right 右侧的字符值
 * @return 返回小于、等于或大于零，表示左值小于、等于或大于右值。
 *         如果类型不匹配，将触发断言错误。
 */
int CharType::compare(const Value &left, const Value &right) const
{
  // 确保输入的值都是字符类型
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");

  // 调用字符串比较函数，返回比较结果
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

/**
 * @brief 从字符串设置字符类型的值
 *
 * @param val 要设置的字符值
 * @param data 输入的字符串数据
 * @return 返回操作结果，成功时返回 RC::SUCCESS。
 */
RC CharType::set_value_from_str(Value &val, const string &data) const
{
  // 将字符串数据设置为值
  val.set_string(data.c_str());
  return RC::SUCCESS;  // 返回成功状态
}

/**
 * @brief 将字符类型值转换为其他类型
 *
 * @param val 输入的字符值
 * @param type 目标属性类型
 * @param result 转换后的值
 * @return 返回操作结果，未实现的转换类型返回 RC::UNIMPLEMENTED。
 */
RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  // 根据目标类型执行转换，当前未实现的类型返回未实现状态
  switch (type) {
    default: return RC::UNIMPLEMENTED;  // 默认情况返回未实现状态
  }
  return RC::SUCCESS;  // 返回成功状态
}

/**
 * @brief 计算字符类型值转换的成本
 *
 * @param type 目标属性类型
 * @return 返回转换的成本，若目标类型是 CHARS，返回 0；否则返回 INT32_MAX。
 */
int CharType::cast_cost(AttrType type)
{
  // 如果目标类型是 CHARS，转换成本为 0
  if (type == AttrType::CHARS) {
    return 0;
  }
  return INT32_MAX;  // 对于其他类型，返回最大整数值作为成本
}

/**
 * @brief 将字符类型的值转换为字符串
 *
 * @param val 输入的字符值
 * @param result 转换后的字符串结果
 * @return 返回操作结果，成功时返回 RC::SUCCESS。
 */
RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;                  // 创建字符串流
  ss << val.value_.pointer_value_;  // 将值写入字符串流
  result = ss.str();                // 获取字符串流中的字符串
  return RC::SUCCESS;               // 返回成功状态
}
