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
// Created by WangYunlai on 2023/06/28.
//

#include "common/value.h"  // 引入 Value 类的头文件

#include "common/lang/comparator.h"  // 引入比较器的头文件
#include "common/lang/exception.h"   // 引入异常处理的头文件
#include "common/lang/sstream.h"     // 引入字符串流的头文件
#include "common/lang/string.h"      // 引入字符串操作的头文件
#include "common/log/log.h"          // 引入日志记录的头文件

/**
 * @brief 构造函数，使用整型值初始化 Value 对象
 * @param val 整型值
 */
Value::Value(int val)
{
  set_int(val);  // 调用 set_int 方法设置整型值
}

/**
 * @brief 构造函数，使用浮点型值初始化 Value 对象
 * @param val 浮点值
 */
Value::Value(float val)
{
  set_float(val);  // 调用 set_float 方法设置浮点值
}

/**
 * @brief 构造函数，使用布尔型值初始化 Value 对象
 * @param val 布尔值
 */
Value::Value(bool val)
{
  set_boolean(val);  // 调用 set_boolean 方法设置布尔值
}

/**
 * @brief 构造函数，使用字符串初始化 Value 对象
 * @param s 字符串指针
 * @param len 字符串长度，默认为0
 */
Value::Value(const char *s, int len /*= 0*/)
{
  set_string(s, len);  // 调用 set_string 方法设置字符串
}

/**
 * @brief 拷贝构造函数，复制另一个 Value 对象
 * @param other 另一个 Value 对象
 */
Value::Value(const Value &other)
{
  this->attr_type_ = other.attr_type_;  // 拷贝属性类型
  this->length_    = other.length_;     // 拷贝数据长度
  this->own_data_  = other.own_data_;   // 拷贝是否拥有数据

  // 根据属性类型，处理字符串的深拷贝
  switch (this->attr_type_) {
    case AttrType::CHARS: {
      set_string_from_other(other);  // 拷贝字符串
    } break;

    default: {
      this->value_ = other.value_;  // 直接拷贝其他类型的值
    } break;
  }
}

/**
 * @brief 移动构造函数
 * @param other 另一个移动的 Value 对象
 */
Value::Value(Value &&other)
{
  this->attr_type_ = other.attr_type_;  // 拷贝属性类型
  this->length_    = other.length_;     // 拷贝数据长度
  this->own_data_  = other.own_data_;   // 拷贝是否拥有数据
  this->value_     = other.value_;      // 拷贝值

  // 使其他对象的资源无效，防止其析构时释放内存
  other.own_data_ = false;
  other.length_   = 0;
}

/**
 * @brief 拷贝赋值运算符
 * @param other 另一个 Value 对象
 * @return 当前对象的引用
 */
Value &Value::operator=(const Value &other)
{
  if (this == &other) {
    return *this;  // 自赋值检查
  }
  reset();  // 清理旧数据

  // 拷贝新数据
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;

  switch (this->attr_type_) {
    case AttrType::CHARS: {
      set_string_from_other(other);  // 拷贝字符串数据
    } break;

    default: {
      this->value_ = other.value_;  // 直接赋值
    } break;
  }
  return *this;
}

/**
 * @brief 移动赋值运算符
 * @param other 另一个 Value 对象
 * @return 当前对象的引用
 */
Value &Value::operator=(Value &&other)
{
  if (this == &other) {
    return *this;  // 自赋值检查
  }
  reset();  // 清理旧数据

  // 拷贝新数据
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;
  this->value_     = other.value_;

  // 使其他对象的资源无效，防止其析构时释放内存
  other.own_data_ = false;
  other.length_   = 0;
  return *this;
}

/**
 * @brief 重置 Value 对象，释放资源并清空数据
 */
void Value::reset()
{
  switch (attr_type_) {
    case AttrType::CHARS:
      if (own_data_ && value_.pointer_value_ != nullptr) {
        delete[] value_.pointer_value_;   // 删除字符串数据
        value_.pointer_value_ = nullptr;  // 置空指针
      }
      break;
    default: break;  // 其他类型无需处理
  }

  // 重置属性
  attr_type_ = AttrType::UNDEFINED;  // 将类型重置为未定义
  length_    = 0;                    // 数据长度重置
  own_data_  = false;                // 不拥有数据
}

/**
 * @brief 设置数据，适用于不同的数据类型
 * @param data 数据指针
 * @param length 数据长度
 */
void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      set_string(data, length);  // 设置字符串数据
    } break;
    case AttrType::INTS: {
      value_.int_value_ = *(int *)data;  // 设置整型数据
      length_           = length;
    } break;
    case AttrType::FLOATS: {
      value_.float_value_ = *(float *)data;  // 设置浮点型数据
      length_             = length;
    } break;
    case AttrType::BOOLEANS: {
      value_.bool_value_ = *(int *)data != 0;  // 设置布尔型数据
      length_            = length;
    } break;
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);  // 处理未知类型
    } break;
  }
}

/**
 * @brief 设置整型值
 * @param val 整型数据
 */
void Value::set_int(int val)
{
  reset();                             // 清空旧数据
  attr_type_        = AttrType::INTS;  // 设置属性类型为整型
  value_.int_value_ = val;             // 设置整型值
  length_           = sizeof(val);     // 设置数据长度
}

/**
 * @brief 设置浮点型值
 * @param val 浮点型数据
 */
void Value::set_float(float val)
{
  reset();                                 // 清空旧数据
  attr_type_          = AttrType::FLOATS;  // 设置属性类型为浮点型
  value_.float_value_ = val;               // 设置浮点值
  length_             = sizeof(val);       // 设置数据长度
}

/**
 * @brief 设置布尔值
 * @param val 布尔型数据
 */
void Value::set_boolean(bool val)
{
  reset();                                  // 清空旧数据
  attr_type_         = AttrType::BOOLEANS;  // 设置属性类型为布尔型
  value_.bool_value_ = val;                 // 设置布尔值
  length_            = sizeof(val);         // 设置数据长度
}

/**
 * @brief 设置字符串值
 * @param s 字符串指针
 * @param len 字符串长度，默认为0表示自动计算长度
 */
void Value::set_string(const char *s, int len /*= 0*/)
{
  reset();                       // 清空旧数据
  attr_type_ = AttrType::CHARS;  // 设置属性类型为字符串

  if (s == nullptr) {  // 处理空字符串
    value_.pointer_value_ = nullptr;
    length_               = 0;
  } else {
    own_data_ = true;  // 标记为拥有数据
    if (len > 0) {
      len = strnlen(s, len);  // 获取指定长度的字符串
    } else {
      len = strlen(s);  // 获取字符串的实际长度
    }
    value_.pointer_value_ = new char[len + 1];  // 分配内存
    length_               = len;                // 设置数据长度
    memcpy(value_.pointer_value_, s, len);      // 拷贝字符串
    value_.pointer_value_[len] = '\0';          // 添加字符串结束符
  }
}

/**
 * @brief 根据另一个 Value 对象设置值
 * @param value 另一个 Value 对象
 */
void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case AttrType::INTS: {
      set_int(value.get_int());  // 设置整型值
    } break;
    case AttrType::FLOATS: {
      set_float(value.get_float());  // 设置浮点型值
    } break;
    case AttrType::CHARS: {
      set_string(value.get_string().c_str());  // 设置字符串值
    } break;
    case AttrType::BOOLEANS: {
      set_boolean(value.get_boolean());  // 设置布尔值
    } break;
    default: {
      ASSERT(false, "got an invalid value type");  // 处理无效值类型
    } break;
  }
}

/**
 * @brief 从其他 Value 对象拷贝字符串数据
 * @param other 另一个 Value 对象
 */
void Value::set_string_from_other(const Value &other)
{
  ASSERT(attr_type_ == AttrType::CHARS, "attr type is not CHARS");  // 确保当前对象类型是 CHARS
  if (own_data_ && other.value_.pointer_value_ != nullptr && length_ != 0) {
    this->value_.pointer_value_ = new char[this->length_ + 1];                        // 分配新内存
    memcpy(this->value_.pointer_value_, other.value_.pointer_value_, this->length_);  // 拷贝字符串
    this->value_.pointer_value_[this->length_] = '\0';                                // 添加结束符
  }
}

/**
 * @brief 获取数据指针
 * @return 数据指针
 */
const char *Value::data() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      return value_.pointer_value_;  // 返回字符串指针
    } break;
    default: {
      return (const char *)&value_;  // 返回其他类型的指针
    } break;
  }
}

/**
 * @brief 将值转换为字符串
 * @return 转换后的字符串
 */
string Value::to_string() const
{
  string res;
  RC     rc = DataType::type_instance(this->attr_type_)->to_string(*this, res);  // 转换为字符串
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to convert value to string. type=%s", attr_type_to_string(this->attr_type_));  // 处理转换失败
    return "";
  }
  return res;  // 返回转换后的字符串
}

/**
 * @brief 比较两个 Value 对象
 * @param other 另一个 Value 对象
 * @return 比较结果
 */
int Value::compare(const Value &other) const
{
  return DataType::type_instance(this->attr_type_)->compare(*this, other);  // 调用类型实例比较
}

/**
 * @brief 获取整型值
 * @return 整型值
 */
int Value::get_int() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return (int)(std::stol(value_.pointer_value_));  // 尝试将字符串转换为整型
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", value_.pointer_value_, ex.what());  // 处理转换异常
        return 0;  // 返回默认值
      }
    }
    case AttrType::INTS: {
      return value_.int_value_;  // 返回整型值
    }
    case AttrType::FLOATS: {
      return (int)(value_.float_value_);  // 将浮点值转换为整型
    }
    case AttrType::BOOLEANS: {
      return (int)(value_.bool_value_);  // 将布尔值转换为整型
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);  // 处理未知类型
      return 0;                                           // 返回默认值
    }
  }
  return 0;  // 默认返回值
}

/**
 * @brief 获取浮点值
 * @return 浮点值
 */
float Value::get_float() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return std::stof(value_.pointer_value_);  // 尝试将字符串转换为浮点值
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", value_.pointer_value_, ex.what());  // 处理转换异常
        return 0.0;                                                                                   // 返回默认值
      }
    } break;
    case AttrType::INTS: {
      return float(value_.int_value_);  // 将整型值转换为浮点值
    } break;
    case AttrType::FLOATS: {
      return value_.float_value_;  // 返回浮点值
    } break;
    case AttrType::BOOLEANS: {
      return float(value_.bool_value_);  // 将布尔值转换为浮点值
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);  // 处理未知类型
      return 0;                                           // 返回默认值
    }
  }
  return 0;  // 默认返回值
}

/**
 * @brief 获取字符串值
 * @return 字符串值
 */
string Value::get_string() const
{
  return this->to_string();  // 调用 to_string 方法获取字符串
}

/**
 * @brief 获取布尔值
 * @return 布尔值
 */
bool Value::get_boolean() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        float val = std::stof(value_.pointer_value_);  // 将字符串转换为浮点值
        if (val >= EPSILON || val <= -EPSILON) {
          return true;  // 非零值返回 true
        }

        int int_val = std::stol(value_.pointer_value_);  // 将字符串转换为整型
        if (int_val != 0) {
          return true;  // 非零值返回 true
        }

        return value_.pointer_value_ != nullptr;  // 处理空指针
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", value_.pointer_value_, ex.what());                           // 处理转换异常
        return value_.pointer_value_ != nullptr;  // 处理空指针
      }
    } break;
    case AttrType::INTS: {
      return value_.int_value_ != 0;  // 整型非零值返回 true
    } break;
    case AttrType::FLOATS: {
      float val = value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;  // 浮点非零值返回 true
    } break;
    case AttrType::BOOLEANS: {
      return value_.bool_value_;  // 返回布尔值
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);  // 处理未知类型
      return false;                                       // 返回 false
    }
  }
  return false;  // 默认返回值
}
