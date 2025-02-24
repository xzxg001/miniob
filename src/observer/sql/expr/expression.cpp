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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"

using namespace std;

/**
 * @brief 获取元组中的字段值。
 *
 * @param tuple 传入的元组对象
 * @param value 用于存储获取到的字段值
 * @return RC 返回操作的状态码
 */
RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  // 通过表名和字段名在元组中找到对应的字段值，并存储到 value 中
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

/**
 * @brief 判断当前表达式与另一个表达式是否相等。
 *
 * @param other 需要比较的另一个表达式
 * @return bool 如果两个表达式相等，返回 true；否则返回 false
 */
bool FieldExpr::equal(const Expression &other) const
{
  // 如果是同一个对象，直接返回 true
  if (this == &other) {
    return true;
  }
  // 如果类型不同，返回 false
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  // 强制转换为 FieldExpr 类型，比较表名和字段名
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

/**
 * @brief 从 chunk 中获取对应列的数据。
 *
 * @param chunk 包含所有列的数据块
 * @param column 用于存储获取到的列数据
 * @return RC 返回操作的状态码
 */
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  // 如果已经获取了列的位置，直接引用对应的列
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    // 如果未记录列的位置，通过字段 ID 获取对应的列
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

/**
 * @brief 判断 ValueExpr 是否与另一个表达式相等。
 *
 * @param other 需要比较的另一个表达式
 * @return bool 如果相等，返回 true；否则返回 false
 */
bool ValueExpr::equal(const Expression &other) const
{
  // 如果是同一个对象，直接返回 true
  if (this == &other) {
    return true;
  }
  // 如果类型不同，返回 false
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  // 强制转换为 ValueExpr 类型，比较内部存储的值是否相等
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

/**
 * @brief 获取表达式的值。
 *
 * @param tuple 传入的元组对象
 * @param value 用于存储获取到的值
 * @return RC 返回操作的状态码
 */
RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  // 将当前值赋给返回的 value
  value = value_;
  return RC::SUCCESS;
}

/**
 * @brief 从 chunk 中获取表达式对应的列。
 *
 * @param chunk 包含所有列的数据块
 * @param column 用于存储获取到的列数据
 * @return RC 返回操作的状态码
 */
RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  // 初始化列的值
  column.init(value_);
  return RC::SUCCESS;
}

/**
 * @brief CastExpr 的构造函数，初始化转换表达式。
 *
 * @param child 子表达式，存储需要转换的值
 * @param cast_type 目标转换类型
 */
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

/**
 * @brief CastExpr 的析构函数。
 */
CastExpr::~CastExpr() {}

/**
 * @brief 将传入的值转换为目标类型。
 *
 * @param value 输入的值
 * @param cast_value 转换后的值
 * @return RC 返回操作的状态码
 */
RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  // 如果值的类型与目标类型相同，直接赋值
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  // 否则进行类型转换
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

/**
 * @brief 获取转换后的值。
 *
 * @param tuple 传入的元组对象
 * @param result 存储转换后的结果
 * @return RC 返回操作的状态码
 */
RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  // 先获取子表达式的值
  RC rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 将值进行转换并返回
  return cast(value, result);
}

/**
 * @brief 尝试获取转换后的值，适用于没有元组输入的场景。
 *
 * @param result 存储转换后的结果
 * @return RC 返回操作的状态码
 */
RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  // 尝试获取子表达式的值
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 将值进行转换并返回
  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

// ComparisonExpr 构造函数，初始化表达式的比较操作符和左右表达式
ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))  // 使用std::move将左、右表达式传递给left_和right_
{}

// ComparisonExpr 析构函数
ComparisonExpr::~ComparisonExpr() {}

// 比较两个Value的值，根据指定的比较操作符得出结果
/**
 * @brief 比较两个Value对象的值，并根据比较操作符得出布尔结果
 * @param left 左操作数的Value
 * @param right 右操作数的Value
 * @param result 存储比较结果的布尔值
 * @return 返回比较操作成功或失败的状态
 */
RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;          // 初始化返回值为成功
  int cmp_result = left.compare(right);  // 调用Value类的compare函数进行比较，返回比较结果
  result         = false;                // 默认比较结果为false

  // 根据不同的比较操作符，设置相应的比较逻辑
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);  // 若比较结果为0，则说明两值相等
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);  // 若比较结果小于或等于0，则左值小于或等于右值
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);  // 若比较结果不等于0，则说明两值不相等
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);  // 若比较结果小于0，则左值小于右值
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);  // 若比较结果大于或等于0，则左值大于或等于右值
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);  // 若比较结果大于0，则左值大于右值
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);  // 若操作符不受支持，记录警告日志
      rc = RC::INTERNAL;                             // 返回内部错误
    } break;
  }

  return rc;  // 返回执行状态
}

// 尝试直接获取比较结果，如果左右表达式都是Value类型，则进行值比较并返回结果
/**
 * @brief 直接比较左右操作数的值并返回结果（用于常量值表达式）
 * @param cell 用于存储比较结果的Value对象
 * @return 返回比较操作的状态
 */
RC ComparisonExpr::try_get_value(Value &cell) const
{
  // 检查左右表达式是否均为VALUE类型
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    // 将左右表达式强制转换为ValueExpr类型
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();   // 获取左操作数的值
    const Value &right_cell       = right_value_expr->get_value();  // 获取右操作数的值

    bool value = false;                                        // 初始化比较结果为false
    RC   rc    = compare_value(left_cell, right_cell, value);  // 调用compare_value进行比较
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));  // 记录比较失败的日志
    } else {
      cell.set_boolean(value);  // 将布尔比较结果存储在cell中
    }
    return rc;  // 返回比较操作的执行状态
  }

  return RC::INVALID_ARGUMENT;  // 若左右表达式不是VALUE类型，则返回无效参数错误
}

// 计算比较表达式的值，基于Tuple对象获取左右表达式的值，并进行比较
/**
 * @brief 计算比较表达式的值，通过从tuple中获取左右操作数并进行比较
 * @param tuple 包含操作数的元组
 * @param value 用于存储最终比较结果的Value对象
 * @return 返回比较操作的状态
 */
RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;   // 存储左操作数的值
  Value right_value;  // 存储右操作数的值

  // 从tuple中获取左操作数的值
  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));  // 若获取失败，记录日志并返回
    return rc;
  }
  // 从tuple中获取右操作数的值
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));  // 若获取失败，记录日志并返回
    return rc;
  }

  bool bool_value = false;  // 初始化比较结果为false

  // 调用compare_value进行比较操作
  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);  // 若比较成功，将布尔结果设置到value中
  }
  return rc;  // 返回执行状态
}

// Evaluation of comparison expression, comparing columns from a chunk and setting the results in select vector
/**
 * @brief 计算比较表达式的值，通过对Chunk中的列进行比较，并将结果存储在select向量中
 * @param chunk 需要进行比较的Chunk对象
 * @param select 存储比较结果的向量
 * @return 返回执行状态
 */
RC ComparisonExpr::eval(Chunk &chunk, std::vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;  // 初始化返回状态为成功
  Column left_column;       // 用于存储左列
  Column right_column;      // 用于存储右列

  // 获取左列的值
  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));  // 获取失败时记录日志
    return rc;                                                            // 返回错误状态
  }

  // 获取右列的值
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));  // 获取失败时记录日志
    return rc;                                                             // 返回错误状态
  }

  // 检查左列与右列的属性类型是否相同
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");  // 如果不同类型，记录警告日志
    return RC::INTERNAL;                                      // 返回内部错误状态
  }

  // 根据列的属性类型进行相应的比较操作
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);  // 如果是整型，调用整型比较函数
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);  // 如果是浮点型，调用浮点型比较函数
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());  // 不支持的类型，记录警告日志
    return RC::INTERNAL;                                           // 返回内部错误状态
  }
  return rc;  // 返回执行状态
}

// Template function for comparing two columns of type T and storing the results
/**
 * @brief 比较两个列的值并将结果存储在指定的结果向量中
 * @tparam T 列数据的类型
 * @param left 左侧列
 * @param right 右侧列
 * @param result 存储比较结果的向量
 * @return 返回执行状态
 */
template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, std::vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  // 判断左右列是否为常量列
  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;   // 左列是否为常量列
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;  // 右列是否为常量列

  // 根据列的类型和常量属性进行比较
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);  // 都是常量列
  } else if (left_const && !right_const) {
    compare_result<T, true, false>(
        (T *)left.data(), (T *)right.data(), right.count(), result, comp_);  // 左为常量，右为变量
  } else if (!left_const && right_const) {
    compare_result<T, false, true>(
        (T *)left.data(), (T *)right.data(), left.count(), result, comp_);  // 左为变量，右为常量
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);  // 都是变量列
  }
  return rc;  // 返回执行状态
}

////////////////////////////////////////////////////////////////////////////////
// Conjunction expression constructor, initializes conjunction type and child expressions
/**
 * @brief 连接表达式构造函数，初始化连接类型和子表达式
 * @param type 连接类型（AND/OR）
 * @param children 子表达式的向量
 */
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))  // 将连接类型和子表达式转移到成员变量中
{}

// 计算连接表达式的值
/**
 * @brief 计算连接表达式的值，通过评估所有子表达式并根据连接类型返回结果
 * @param tuple 包含表达式计算所需数据的元组
 * @param value 用于存储最终计算结果的Value对象
 * @return 返回执行状态
 */
RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  // 若没有子表达式，则返回默认值为真
  if (children_.empty()) {
    value.set_boolean(true);  // 连接表达式没有子表达式时，结果为true
    return rc;                // 返回成功状态
  }

  Value tmp_value;  // 临时存储子表达式结果
  // 遍历所有子表达式
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);  // 获取当前子表达式的值
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));  // 获取失败时记录日志
      return rc;                                                             // 返回错误状态
    }

    bool bool_value = tmp_value.get_boolean();  // 获取布尔值

    // 根据连接类型进行判断，更新结果
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);  // 如果是AND且有一个为假，或是OR且有一个为真，立即返回结果
      return rc;                      // 返回成功状态
    }
  }

  // 若所有子表达式均为真，返回对应的默认值
  bool default_value = (conjunction_type_ == Type::AND);  // AND的默认值为true，OR的默认值为false
  value.set_boolean(default_value);                       // 设置最终结果
  return rc;                                              // 返回成功状态
}

////////////////////////////////////////////////////////////////////////////////

// Arithmetic expression implementation for evaluating basic arithmetic operations
/**
 * @brief 算术表达式构造函数，使用给定的类型、左侧和右侧表达式进行初始化
 * @param type 算术操作类型（如加、减、乘、除）
 * @param left 左侧表达式
 * @param right 右侧表达式
 */
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)  // 初始化成员变量
{}

/**
 * @brief 算术表达式构造函数，使用给定的类型、左侧和右侧唯一指针表达式进行初始化
 * @param type 算术操作类型（如加、减、乘、除）
 * @param left 左侧唯一指针表达式
 * @param right 右侧唯一指针表达式
 */
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))  // 初始化成员变量
{}

/**
 * @brief 判断当前算术表达式是否与另一个表达式相等
 * @param other 另一个表达式
 * @return 如果相等则返回true，否则返回false
 */
bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;  // 如果是同一对象，返回true
  }
  if (type() != other.type()) {
    return false;  // 如果类型不同，返回false
  }
  // 将other转换为ArithmeticExpr引用
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  // 检查算术类型和左右表达式是否相等
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}

/**
 * @brief 获取当前算术表达式的值类型
 * @return 返回值的属性类型
 */
AttrType ArithmeticExpr::value_type() const
{
  // 如果右侧表达式为空，直接返回左侧表达式的值类型
  if (!right_) {
    return left_->value_type();
  }

  // 检查左右表达式的值类型，并返回适当的值类型
  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;  // 如果都是整型且不是除法，返回整型
  }

  return AttrType::FLOATS;  // 其他情况返回浮点型
}

/**
 * @brief 计算给定左值和右值的结果并将其存储在值对象中
 * @param left_value 左侧值
 * @param right_value 右侧值
 * @param value 结果值
 * @return 返回执行状态
 */
RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  const AttrType target_type = value_type();  // 获取目标类型
  value.set_type(target_type);                // 设置值的类型

  // 根据算术类型执行相应的操作
  switch (arithmetic_type_) {
    case Type::ADD: {  // 加法
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {  // 减法
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {  // 乘法
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {  // 除法
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {  // 负值
      Value::negative(left_value, value);
    } break;

    default: {                                                       // 不支持的算术类型
      rc = RC::INTERNAL;                                             // 设置返回状态为内部错误
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);  // 记录警告日志
    } break;
  }
  return rc;  // 返回执行状态
}

// 算术表达式执行计算的模板函数
/**
 * @brief 执行给定类型的算术操作，并将结果存储在结果列中
 *
 * @tparam LEFT_CONSTANT 是否左侧列是常量
 * @tparam RIGHT_CONSTANT 是否右侧列是常量
 * @param left 左侧列
 * @param right 右侧列
 * @param result 结果列
 * @param type 算术操作类型（加、减、乘、除、取负）
 * @param attr_type 列的属性类型（整型或浮点型）
 * @return 执行状态
 */
template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;                    // 初始化返回状态为成功
  switch (type) {                         // 根据给定的算术操作类型执行相应的操作
    case Type::ADD: {                     // 加法
      if (attr_type == AttrType::INTS) {  // 整型处理
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(  // 调用二元操作函数
            (int *)left.data(),
            (int *)right.data(),
            (int *)result.data(),
            result.capacity());
      } else if (attr_type == AttrType::FLOATS) {  // 浮点型处理
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;  // 不支持的属性类型
      }
    } break;
    case Type::SUB:  // 减法
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;  // 不支持的属性类型
      }
      break;
    case Type::MUL:  // 乘法
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;  // 不支持的属性类型
      }
      break;
    case Type::DIV:  // 除法
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;  // 不支持的属性类型
      }
      break;
    case Type::NEGATIVE:  // 取负
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;  // 不支持的属性类型
      }
      break;
    default:
      rc = RC::UNIMPLEMENTED;  // 不支持的操作类型
      break;
  }

  // 如果执行成功，设置结果列的计数
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }

  return rc;  // 返回执行状态
}

/**
 * @brief 获取当前表达式的值，并将其存储在值对象中
 *
 * @param tuple 当前元组
 * @param value 存储计算结果的值对象
 * @return 执行状态
 */
RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  Value left_value;   // 左侧值对象
  Value right_value;  // 右侧值对象

  // 获取左侧表达式的值
  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));  // 记录警告日志
    return rc;                                                            // 返回错误状态
  }

  // 获取右侧表达式的值
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));  // 记录警告日志
    return rc;                                                             // 返回错误状态
  }

  // 调用calc_value计算结果
  return calc_value(left_value, right_value, value);
}

// 获取算术表达式对应的列
/**
 * @brief 从给定的块中获取算术表达式对应的列
 *
 * @param chunk 数据块
 * @param column 结果列
 * @return 执行状态
 */
RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功
  if (pos_ != -1) {     // 如果 pos_ 被设置，直接引用相应列
    column.reference(chunk.column(pos_));
    return rc;  // 返回成功状态
  }

  // 如果 pos_ 未设置，则需要计算左右表达式的列
  Column left_column;
  Column right_column;

  // 获取左侧表达式对应的列
  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));  // 记录警告日志
    return rc;                                                             // 返回错误状态
  }

  // 获取右侧表达式对应的列
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));  // 记录警告日志
    return rc;                                                              // 返回错误状态
  }

  // 计算并返回算术列
  return calc_column(left_column, right_column, column);
}

/**
 * @brief 计算两个列的算术结果并存储在结果列中
 *
 * @param left_column 左侧列
 * @param right_column 右侧列
 * @param column 结果列
 * @return 执行状态
 */
RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  const AttrType target_type = value_type();  // 获取目标属性类型
  // 初始化结果列
  column.init(target_type, left_column.attr_len(), std::max(left_column.count(), right_column.count()));

  // 判断左右列是否为常量列
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;

  // 根据左右列是否为常量列，选择不同的计算路径
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);  // 设置结果列为常量列
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);  // 设置结果列为普通列
    if (left_const) {
      rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
    } else if (right_const) {
      rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
    } else {
      rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
    }
  }

  return rc;  // 返回执行状态
}

/**
 * @brief 尝试获取当前表达式的值
 *
 * @param value 存储计算结果的值对象
 * @return 执行状态
 */
RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功

  Value left_value;   // 左侧值对象
  Value right_value;  // 右侧值对象

  // 尝试获取左侧表达式的值
  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));  // 记录警告日志
    return rc;                                                            // 返回错误状态
  }

  // 如果存在右侧表达式，尝试获取其值
  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));  // 记录警告日志
      return rc;                                                             // 返回错误状态
    }
  }

  // 计算并返回结果值
  return calc_value(left_value, right_value, value);
}

// 未绑定聚合表达式构造函数
UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

// 聚合表达式构造函数
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

/**
 * @brief 获取聚合表达式对应的列
 *
 * @param chunk 数据块
 * @param column 结果列
 * @return 执行状态
 */
RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功
  if (pos_ != -1) {     // 如果 pos_ 被设置，直接引用相应列
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;  // 如果 pos_ 未设置，返回内部错误
  }
  return rc;  // 返回执行状态
}

/**
 * @brief 比较当前聚合表达式与另一个表达式是否相等
 *
 * @param other 另一个表达式对象
 * @return 如果相等则返回 true，否则返回 false
 */
bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;  // 如果是同一个对象，返回 true
  }
  if (other.type() != type()) {
    return false;  // 如果类型不一致，返回 false
  }

  // 将 other 转换为 AggregateExpr 类型
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);

  // 比较聚合类型和子表达式的相等性
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

/**
 * @brief 创建聚合器对象
 *
 * @return 唯一指针指向新创建的聚合器对象
 */
unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;  // 声明聚合器指针
  switch (aggregate_type_) {
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();  // 创建求和聚合器
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");  // 对于不支持的聚合类型，触发断言
      break;
    }
  }
  return aggregator;  // 返回聚合器指针
}

/**
 * @brief 从元组中获取聚合表达式的值
 *
 * @param tuple 数据元组
 * @param value 存储获取到的值
 * @return 执行状态
 */
RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);  // 在元组中查找单元格并获取值
}

/**
 * @brief 将字符串转换为聚合表达式类型
 *
 * @param type_str 类型字符串
 * @param type 输出参数，用于存储转换后的聚合类型
 * @return 执行状态
 */
RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;  // 初始化返回状态为成功
  // 根据输入字符串转换聚合类型
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;  // 无效的聚合类型字符串
  }
  return rc;  // 返回执行状态
}
