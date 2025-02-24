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
// Created by Wangyunlai 2023/6/27
//

#pragma once  // 防止头文件被重复包含

#include "common/lang/string.h"     // 引入字符串相关的头文件
#include "common/lang/memory.h"     // 引入内存管理相关的头文件
#include "common/type/attr_type.h"  // 引入属性类型定义
#include "common/type/data_type.h"  // 引入数据类型定义

/**
 * @brief 属性的值
 * @ingroup DataType
 * @details 与 DataType（数据类型）配套完成各种算术运算、比较、类型转换等操作。
 * 此类同时记录了数据的值与类型。当需要对值进行运算时，建议使用类似 Value::add 的操作而不是 DataType::add。
 * 在进行运算前，应设置好结果的类型，例如进行两个 INT 类型的除法运算时，结果类型应该设置为 FLOAT。
 */
class Value final
{
public:
  // 友元类，用于访问私有成员
  friend class DataType;
  friend class IntegerType;
  friend class FloatType;
  friend class BooleanType;
  friend class CharType;

  // 默认构造函数
  Value() = default;

  // 析构函数，调用 reset() 方法释放资源
  ~Value() { reset(); }

  /**
   * @brief 带参构造函数
   * @param attr_type 属性类型
   * @param data 数据指针
   * @param length 数据长度，默认为 4
   */
  Value(AttrType attr_type, char *data, int length = 4) : attr_type_(attr_type) { this->set_data(data, length); }

  // 显式构造函数，支持不同类型的初始化
  explicit Value(int val);
  explicit Value(float val);
  explicit Value(bool val);
  explicit Value(const char *s, int len = 0);

  // 拷贝构造函数
  Value(const Value &other);
  // 移动构造函数
  Value(Value &&other);

  // 拷贝赋值运算符
  Value &operator=(const Value &other);
  // 移动赋值运算符
  Value &operator=(Value &&other);

  /**
   * @brief 重置当前值
   * @details 清空当前值并释放内存，如果有需要的话
   */
  void reset();

  /**
   * @brief 加法运算
   * @param left 左侧操作数
   * @param right 右侧操作数
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC add(const Value &left, const Value &right, Value &result)
  {
    return DataType::type_instance(result.attr_type())->add(left, right, result);
  }

  /**
   * @brief 减法运算
   * @param left 左侧操作数
   * @param right 右侧操作数
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC subtract(const Value &left, const Value &right, Value &result)
  {
    return DataType::type_instance(result.attr_type())->subtract(left, right, result);
  }

  /**
   * @brief 乘法运算
   * @param left 左侧操作数
   * @param right 右侧操作数
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC multiply(const Value &left, const Value &right, Value &result)
  {
    return DataType::type_instance(result.attr_type())->multiply(left, right, result);
  }

  /**
   * @brief 除法运算
   * @param left 左侧操作数
   * @param right 右侧操作数
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC divide(const Value &left, const Value &right, Value &result)
  {
    return DataType::type_instance(result.attr_type())->divide(left, right, result);
  }

  /**
   * @brief 取负运算
   * @param value 输入值
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC negative(const Value &value, Value &result)
  {
    return DataType::type_instance(result.attr_type())->negative(value, result);
  }

  /**
   * @brief 类型转换
   * @param value 输入值
   * @param to_type 目标类型
   * @param result 存放结果的引用
   * @return RC 返回操作结果码
   */
  static RC cast_to(const Value &value, AttrType to_type, Value &result)
  {
    return DataType::type_instance(value.attr_type())->cast_to(value, to_type, result);
  }

  // 设置属性类型
  void set_type(AttrType type) { this->attr_type_ = type; }

  /**
   * @brief 设置数据
   * @param data 数据指针
   * @param length 数据长度
   */
  void set_data(char *data, int length);

  /**
   * @brief 设置数据（重载）
   * @param data 数据指针
   * @param length 数据长度
   */
  void set_data(const char *data, int length) { this->set_data(const_cast<char *>(data), length); }

  /**
   * @brief 设置值
   * @param value 另一个 Value 对象
   */
  void set_value(const Value &value);

  /**
   * @brief 设置布尔值
   * @param val 布尔值
   */
  void set_boolean(bool val);

  /**
   * @brief 将值转换为字符串
   * @return 返回转换后的字符串
   */
  string to_string() const;

  /**
   * @brief 比较两个值
   * @param other 另一个 Value 对象
   * @return 返回比较结果
   */
  int compare(const Value &other) const;

  /**
   * @brief 获取数据指针
   * @return 返回数据指针
   */
  const char *data() const;

  /**
   * @brief 获取长度
   * @return 返回数据长度
   */
  int length() const { return length_; }

  /**
   * @brief 获取属性类型
   * @return 返回属性类型
   */
  AttrType attr_type() const { return attr_type_; }

public:
  /**
   * @brief 获取整数值
   * @return 返回整数值
   */
  int get_int() const;

  /**
   * @brief 获取浮点值
   * @return 返回浮点值
   */
  float get_float() const;

  /**
   * @brief 获取字符串值
   * @return 返回字符串值
   */
  string get_string() const;

  /**
   * @brief 获取布尔值
   * @return 返回布尔值
   */
  bool get_boolean() const;

private:
  // 设置整数值
  void set_int(int val);

  // 设置浮点值
  void set_float(float val);

  /**
   * @brief 设置字符串值
   * @param s 字符串指针
   * @param len 字符串长度
   */
  void set_string(const char *s, int len = 0);

  /**
   * @brief 从其他 Value 设置字符串值
   * @param other 另一个 Value 对象
   */
  void set_string_from_other(const Value &other);

private:
  AttrType attr_type_ = AttrType::UNDEFINED;  // 属性类型，默认未定义
  int      length_    = 0;                    // 数据长度

  union Val  // 联合体，存储不同类型的值
  {
    int32_t int_value_;          // 整数值
    float   float_value_;        // 浮点值
    bool    bool_value_;         // 布尔值
    char   *pointer_value_;      // 字符串指针
  } value_ = {.int_value_ = 0};  // 默认值为 0

  /// 是否申请并占有内存, 目前对于 CHARS 类型 own_data_ 为 true，其余类型 own_data_ 为 false
  bool own_data_ = false;  // 是否拥有数据的内存
};
