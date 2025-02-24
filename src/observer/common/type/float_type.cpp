#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/float_type.h"
#include "common/value.h"
#include "common/lang/limits.h"

/**
 * @brief 比较两个浮点数值
 *
 * @param left 左侧浮点数值
 * @param right 右侧浮点数值
 * @return
 *  -1 表示 left < right
 *  0 表示 left = right
 *  1 表示 left > right
 *  INT32_MAX 表示未实现的比较
 */
int FloatType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::FLOATS, "left type is not float");
  ASSERT(right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS, "right type is not numeric");

  float left_val  = left.get_float();   // 获取左侧值的浮点数值
  float right_val = right.get_float();  // 获取右侧值的浮点数值

  return common::compare_float((void *)&left_val, (void *)&right_val);  // 使用比较函数返回比较结果
}

/**
 * @brief 将两个浮点数值相加
 *
 * @param left 左侧浮点数值
 * @param right 右侧浮点数值
 * @param result 保存相加结果的值
 * @return 操作结果，成功返回 RC::SUCCESS
 */
RC FloatType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() + right.get_float());  // 计算相加结果
  return RC::SUCCESS;                                      // 返回成功状态
}

/**
 * @brief 将两个浮点数值相减
 *
 * @param left 左侧浮点数值
 * @param right 右侧浮点数值
 * @param result 保存相减结果的值
 * @return 操作结果，成功返回 RC::SUCCESS
 */
RC FloatType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() - right.get_float());  // 计算相减结果
  return RC::SUCCESS;                                      // 返回成功状态
}

/**
 * @brief 将两个浮点数值相乘
 *
 * @param left 左侧浮点数值
 * @param right 右侧浮点数值
 * @param result 保存相乘结果的值
 * @return 操作结果，成功返回 RC::SUCCESS
 */
RC FloatType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() * right.get_float());  // 计算相乘结果
  return RC::SUCCESS;                                      // 返回成功状态
}

/**
 * @brief 将两个浮点数值相除
 *
 * @param left 左侧浮点数值
 * @param right 右侧浮点数值
 * @param result 保存相除结果的值
 * @return 操作结果，成功返回 RC::SUCCESS
 * @note 如果右侧浮点数值接近于零，返回浮点数的最大值以避免除零异常
 */
RC FloatType::divide(const Value &left, const Value &right, Value &result) const
{
  // 检查右侧值是否接近于零，防止除零异常
  if (right.get_float() > -EPSILON && right.get_float() < EPSILON) {
    // 如果接近于零，返回浮点数的最大值作为错误处理
    result.set_float(numeric_limits<float>::max());
  } else {
    result.set_float(left.get_float() / right.get_float());  // 计算相除结果
  }
  return RC::SUCCESS;  // 返回成功状态
}

/**
 * @brief 取浮点数值的负值
 *
 * @param val 输入浮点数值
 * @param result 保存负值的值
 * @return 操作结果，成功返回 RC::SUCCESS
 */
RC FloatType::negative(const Value &val, Value &result) const
{
  result.set_float(-val.get_float());  // 计算负值
  return RC::SUCCESS;                  // 返回成功状态
}

/**
 * @brief 从字符串设置浮点数值
 *
 * @param val 保存解析后浮点数值的值
 * @param data 输入的字符串数据
 * @return 操作结果，成功返回 RC::SUCCESS；如果解析失败，返回 RC::SCHEMA_FIELD_TYPE_MISMATCH
 */
RC FloatType::set_value_from_str(Value &val, const string &data) const
{
  RC           rc = RC::SUCCESS;
  stringstream deserialize_stream;  // 用于解析字符串
  deserialize_stream.clear();       // 清理stream的状态
  deserialize_stream.str(data);     // 设置输入字符串

  float float_value;
  deserialize_stream >> float_value;  // 解析浮点数值
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;  // 如果解析失败，返回类型不匹配错误
  } else {
    val.set_float(float_value);  // 设置解析后的浮点数值
  }
  return rc;  // 返回结果
}

/**
 * @brief 将浮点数值转换为字符串
 *
 * @param val 输入浮点数值
 * @param result 保存结果字符串
 * @return 操作结果，成功返回 RC::SUCCESS
 */
RC FloatType::to_string(const Value &val, string &result) const
{
  stringstream ss;                                       // 创建字符串流
  ss << common::double_to_str(val.value_.float_value_);  // 将浮点数值转换为字符串
  result = ss.str();                                     // 保存结果字符串
  return RC::SUCCESS;                                    // 返回成功状态
}
