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

#pragma once

#include <memory>
#include <string>

#include "common/value.h"
#include "storage/field/field.h"
#include "sql/expr/aggregator.h"
#include "storage/common/chunk.h"

class Tuple;

/**
 * @defgroup Expression
 * @brief 表达式
 */

/**
 * @brief 表达式类型
 * @ingroup Expression
 */
enum class ExprType
{
  NONE,
  STAR,                 ///< 星号，表示所有字段
  UNBOUND_FIELD,        ///< 未绑定的字段，需要在resolver阶段解析为FieldExpr
  UNBOUND_AGGREGATION,  ///< 未绑定的聚合函数，需要在resolver阶段解析为AggregateExpr

  FIELD,        ///< 字段。在实际执行时，根据行数据内容提取对应字段的值
  VALUE,        ///< 常量值
  CAST,         ///< 需要做类型转换的表达式
  COMPARISON,   ///< 需要做比较的表达式
  CONJUNCTION,  ///< 多个表达式使用同一种关系(AND或OR)来联结
  ARITHMETIC,   ///< 算术运算
  AGGREGATION,  ///< 聚合运算
};

/**
 * @brief 表达式的抽象描述
 * @ingroup Expression
 * @details 在SQL的元素中，任何需要得出值的元素都可以使用表达式来描述
 * 比如获取某个字段的值、比较运算、类型转换
 * 当然还有一些当前没有实现的表达式，比如算术运算。
 *
 * 通常表达式的值，是在真实的算子运算过程中，拿到具体的tuple后
 * 才能计算出来真实的值。但是有些表达式可能就表示某一个固定的
 * 值，比如ValueExpr。
 *
 * TODO 区分unbound和bound的表达式
 */
class Expression
{
public:
  Expression()          = default;
  virtual ~Expression() = default;

  /**
   * @brief 判断两个表达式是否相等
   */
  virtual bool equal(const Expression &other) const { return false; }
  /**
   * @brief 根据具体的tuple，来计算当前表达式的值。tuple有可能是一个具体某个表的行数据
   */
  virtual RC get_value(const Tuple &tuple, Value &value) const = 0;

  /**
   * @brief 在没有实际运行的情况下，也就是无法获取tuple的情况下，尝试获取表达式的值
   * @details 有些表达式的值是固定的，比如ValueExpr，这种情况下可以直接获取值
   */
  virtual RC try_get_value(Value &value) const { return RC::UNIMPLEMENTED; }

  /**
   * @brief 从 `chunk` 中获取表达式的计算结果 `column`
   */
  virtual RC get_column(Chunk &chunk, Column &column) { return RC::UNIMPLEMENTED; }

  /**
   * @brief 表达式的类型
   * 可以根据表达式类型来转换为具体的子类
   */
  virtual ExprType type() const = 0;

  /**
   * @brief 表达式值的类型
   * @details 一个表达式运算出结果后，只有一个值
   */
  virtual AttrType value_type() const = 0;

  /**
   * @brief 表达式值的长度
   */
  virtual int value_length() const { return -1; }

  /**
   * @brief 表达式的名字，比如是字段名称，或者用户在执行SQL语句时输入的内容
   */
  virtual const char *name() const { return name_.c_str(); }
  virtual void        set_name(std::string name) { name_ = name; }

  /**
   * @brief 表达式在下层算子返回的 chunk 中的位置
   */
  virtual int  pos() const { return pos_; }
  virtual void set_pos(int pos) { pos_ = pos; }

  /**
   * @brief 用于 ComparisonExpr 获得比较结果 `select`。
   */
  virtual RC eval(Chunk &chunk, std::vector<uint8_t> &select) { return RC::UNIMPLEMENTED; }

protected:
  /**
   * @brief 表达式在下层算子返回的 chunk 中的位置
   * @details 当 pos_ = -1 时表示下层算子没有在返回的 chunk 中计算出该表达式的计算结果，
   * 当 pos_ >= 0时表示在下层算子中已经计算出该表达式的值（比如聚合表达式），且该表达式对应的结果位于
   * chunk 中 下标为 pos_ 的列中。
   */
  int pos_ = -1;

private:
  std::string name_;
};

/**
 * @brief 表达式类 `StarExpr` 用于表示选择所有字段的操作，常用于 `SELECT *` 语句。
 * @ingroup Expression
 */
class StarExpr : public Expression
{
public:
  /**
   * @brief 默认构造函数，创建一个没有指定表名的 `StarExpr`。
   */
  StarExpr() : table_name_() {}

  /**
   * @brief 带表名的构造函数，创建一个 `StarExpr` 表达式。
   * @param table_name 表的名称。
   */
  StarExpr(const char *table_name) : table_name_(table_name) {}

  /**
   * @brief 默认析构函数。
   */
  virtual ~StarExpr() = default;

  /**
   * @brief 获取表达式类型。
   * @return 返回表达式类型 `ExprType::STAR`。
   */
  ExprType type() const override { return ExprType::STAR; }

  /**
   * @brief 获取表达式的值类型。
   * @return 返回未定义类型 `AttrType::UNDEFINED`。
   */
  AttrType value_type() const override { return AttrType::UNDEFINED; }

  /**
   * @brief 获取表达式的值，本类中该方法不需要实现。
   * @param tuple 元组引用。
   * @param[out] value 用于存储结果值的引用。
   * @return 返回 `RC::UNIMPLEMENTED`，表明此方法未实现。
   */
  RC get_value(const Tuple &tuple, Value &value) const override { return RC::UNIMPLEMENTED; }  // 不需要实现

  /**
   * @brief 获取表名。
   * @return 返回表名字符串。
   */
  const char *table_name() const { return table_name_.c_str(); }

private:
  std::string table_name_;  ///< 表名字符串
};

/**
 * @brief 表达式类 `UnboundFieldExpr` 用于表示一个未绑定的字段，即字段在具体表定义中还未绑定。
 * @ingroup Expression
 */
class UnboundFieldExpr : public Expression
{
public:
  /**
   * @brief 构造函数，用于创建带表名和字段名的 `UnboundFieldExpr`。
   * @param table_name 表的名称。
   * @param field_name 字段的名称。
   */
  UnboundFieldExpr(const std::string &table_name, const std::string &field_name)
      : table_name_(table_name), field_name_(field_name)
  {}

  /**
   * @brief 默认析构函数。
   */
  virtual ~UnboundFieldExpr() = default;

  /**
   * @brief 获取表达式类型。
   * @return 返回表达式类型 `ExprType::UNBOUND_FIELD`。
   */
  ExprType type() const override { return ExprType::UNBOUND_FIELD; }

  /**
   * @brief 获取表达式的值类型。
   * @return 返回未定义类型 `AttrType::UNDEFINED`。
   */
  AttrType value_type() const override { return AttrType::UNDEFINED; }

  /**
   * @brief 获取表达式的值，此方法在本类中未实现。
   * @param tuple 元组引用。
   * @param[out] value 用于存储结果值的引用。
   * @return 返回 `RC::INTERNAL`。
   */
  RC get_value(const Tuple &tuple, Value &value) const override { return RC::INTERNAL; }

  /**
   * @brief 获取表名。
   * @return 返回表名字符串。
   */
  const char *table_name() const { return table_name_.c_str(); }

  /**
   * @brief 获取字段名。
   * @return 返回字段名字符串。
   */
  const char *field_name() const { return field_name_.c_str(); }

private:
  std::string table_name_;  ///< 表名字符串
  std::string field_name_;  ///< 字段名字符串
};

/**
 * @brief 表达式类 `FieldExpr` 用于表示已绑定的字段，用于访问特定表的字段。
 * @ingroup Expression
 */
class FieldExpr : public Expression
{
public:
  /**
   * @brief 默认构造函数。
   */
  FieldExpr() = default;

  /**
   * @brief 构造函数，使用表和字段元数据初始化 `FieldExpr`。
   * @param table 表的指针。
   * @param field 字段元数据的指针。
   */
  FieldExpr(const Table *table, const FieldMeta *field) : field_(table, field) {}

  /**
   * @brief 构造函数，使用字段对象初始化 `FieldExpr`。
   * @param field 字段对象。
   */
  FieldExpr(const Field &field) : field_(field) {}

  /**
   * @brief 默认析构函数。
   */
  virtual ~FieldExpr() = default;

  /**
   * @brief 判断当前表达式是否与其他表达式相等。
   * @param other 要比较的另一个表达式。
   * @return 若相等返回 `true`，否则返回 `false`。
   */
  bool equal(const Expression &other) const override;

  /**
   * @brief 获取表达式类型。
   * @return 返回表达式类型 `ExprType::FIELD`。
   */
  ExprType type() const override { return ExprType::FIELD; }

  /**
   * @brief 获取字段的值类型。
   * @return 返回字段的属性类型。
   */
  AttrType value_type() const override { return field_.attr_type(); }

  /**
   * @brief 获取字段的值长度。
   * @return 返回字段长度。
   */
  int value_length() const override { return field_.meta()->len(); }

  /**
   * @brief 获取字段对象的引用。
   * @return 返回字段的引用。
   */
  Field &field() { return field_; }

  /**
   * @brief 获取字段对象的常量引用。
   * @return 返回字段的常量引用。
   */
  const Field &field() const { return field_; }

  /**
   * @brief 获取字段所在的表名。
   * @return 返回表名字符串。
   */
  const char *table_name() const { return field_.table_name(); }

  /**
   * @brief 获取字段名。
   * @return 返回字段名字符串。
   */
  const char *field_name() const { return field_.field_name(); }

  /**
   * @brief 获取特定字段的列数据。
   * @param chunk 数据块。
   * @param[out] column 用于存储的列数据。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_column(Chunk &chunk, Column &column) override;

  /**
   * @brief 获取字段表达式在指定元组中的值。
   * @param tuple 元组引用。
   * @param[out] value 用于存储结果值的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

private:
  Field field_;  ///< 字段对象，表示表达式对应的字段信息
};

/**
 * @brief 表达式类 `ValueExpr` 用于表示常量值表达式。
 * @ingroup Expression
 */
class ValueExpr : public Expression
{
public:
  /**
   * @brief 默认构造函数。
   */
  ValueExpr() = default;

  /**
   * @brief 带值的构造函数，使用指定的 `Value` 初始化 `ValueExpr`。
   * @param value 表达式的常量值。
   */
  explicit ValueExpr(const Value &value) : value_(value) {}

  /**
   * @brief 默认析构函数。
   */
  virtual ~ValueExpr() = default;

  /**
   * @brief 判断当前表达式是否与其他表达式相等。
   * @param other 要比较的另一个表达式。
   * @return 若相等返回 `true`，否则返回 `false`。
   */
  bool equal(const Expression &other) const override;

  /**
   * @brief 获取表达式的值。
   * @param tuple 元组引用。
   * @param[out] value 用于存储结果值的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 将表达式的值存储到列中。
   * @param chunk 数据块。
   * @param[out] column 用于存储的列数据。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_column(Chunk &chunk, Column &column) override;

  /**
   * @brief 尝试获取常量值，无需依赖元组。
   * @param[out] value 用于存储结果值的引用。
   * @return 成功时返回 `RC::SUCCESS`。
   */
  RC try_get_value(Value &value) const override
  {
    value = value_;
    return RC::SUCCESS;
  }

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::VALUE`。
   */
  ExprType type() const override { return ExprType::VALUE; }

  /**
   * @brief 获取表达式的值类型。
   * @return 返回常量值的属性类型。
   */
  AttrType value_type() const override { return value_.attr_type(); }

  /**
   * @brief 获取表达式值的长度。
   * @return 返回值的长度。
   */
  int value_length() const override { return value_.length(); }

  /**
   * @brief 获取常量值。
   * @param[out] value 用于存储值的引用。
   */
  void get_value(Value &value) const { value = value_; }

  /**
   * @brief 获取常量值的引用。
   * @return 返回常量值的引用。
   */
  const Value &get_value() const { return value_; }

private:
  Value value_;  ///< 表达式的常量值
};

/**
 * @brief 类型转换表达式类 `CastExpr`，用于将一个表达式的值类型转换为指定的目标类型。
 * @ingroup Expression
 */
class CastExpr : public Expression
{
public:
  /**
   * @brief 构造函数，初始化 `CastExpr` 为子表达式的值转换成目标类型。
   * @param child 需要转换的子表达式。
   * @param cast_type 转换后的目标类型。
   */
  CastExpr(std::unique_ptr<Expression> child, AttrType cast_type);

  /**
   * @brief 默认析构函数。
   */
  virtual ~CastExpr();

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::CAST`。
   */
  ExprType type() const override { return ExprType::CAST; }

  /**
   * @brief 获取转换后的值。
   * @param tuple 元组引用。
   * @param[out] value 存储转换结果值的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 尝试获取表达式的值，无需依赖元组。
   * @param[out] value 存储转换后的值。
   * @return 成功时返回 `RC::SUCCESS`。
   */
  RC try_get_value(Value &value) const override;

  /**
   * @brief 获取转换后的类型。
   * @return 返回目标属性类型。
   */
  AttrType value_type() const override { return cast_type_; }

  /**
   * @brief 获取子表达式的引用。
   * @return 返回子表达式的引用。
   */
  std::unique_ptr<Expression> &child() { return child_; }

private:
  /**
   * @brief 将指定值转换为目标类型。
   * @param value 要转换的值。
   * @param[out] cast_value 存储转换结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC cast(const Value &value, Value &cast_value) const;

private:
  std::unique_ptr<Expression> child_;      ///< 子表达式指针
  AttrType                    cast_type_;  ///< 转换后的目标类型
};

/**
 * @brief 比较表达式类 `ComparisonExpr`，用于实现两个表达式之间的比较操作。
 * @ingroup Expression
 */
class ComparisonExpr : public Expression
{
public:
  /**
   * @brief 构造函数，初始化比较操作符以及左右操作数。
   * @param comp 比较操作符。
   * @param left 左操作数。
   * @param right 右操作数。
   */
  ComparisonExpr(CompOp comp, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);

  /**
   * @brief 默认析构函数。
   */
  virtual ~ComparisonExpr();

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::COMPARISON`。
   */
  ExprType type() const override { return ExprType::COMPARISON; }

  /**
   * @brief 获取比较结果的值。
   * @param tuple 元组引用。
   * @param[out] value 存储比较结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 获取比较表达式的值类型。
   * @return 返回布尔类型 `AttrType::BOOLEANS`。
   */
  AttrType value_type() const override { return AttrType::BOOLEANS; }

  /**
   * @brief 获取比较操作符。
   * @return 返回比较操作符。
   */
  CompOp comp() const { return comp_; }

  /**
   * @brief 在数据块 `chunk` 中基于 `ComparisonExpr` 获取 `select` 结果。
   * `select` 长度与 `chunk` 行数相同，表示每一行在 `ComparisonExpr` 计算后是否会被输出。
   * @param chunk 数据块。
   * @param[out] select 存储每行选择状态的向量。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC eval(Chunk &chunk, std::vector<uint8_t> &select) override;

  /**
   * @brief 获取左操作数。
   * @return 返回左操作数的引用。
   */
  std::unique_ptr<Expression> &left() { return left_; }

  /**
   * @brief 获取右操作数。
   * @return 返回右操作数的引用。
   */
  std::unique_ptr<Expression> &right() { return right_; }

  /**
   * @brief 尝试在不依赖元组的情况下获取当前表达式的值，通常在优化时使用。
   * @param[out] value 存储结果的引用。
   * @return 成功时返回 `RC::SUCCESS`。
   */
  RC try_get_value(Value &value) const override;

  /**
   * @brief 比较两个值。
   * @param left 左值。
   * @param right 右值。
   * @param[out] value 存储比较结果的布尔值。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC compare_value(const Value &left, const Value &right, bool &value) const;

  /**
   * @brief 比较两个列的值，用于批量计算。
   * @param left 左列。
   * @param right 右列。
   * @param[out] result 存储每行比较结果的向量。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  template <typename T>
  RC compare_column(const Column &left, const Column &right, std::vector<uint8_t> &result) const;

private:
  CompOp                      comp_;   ///< 比较操作符
  std::unique_ptr<Expression> left_;   ///< 左操作数表达式
  std::unique_ptr<Expression> right_;  ///< 右操作数表达式
};

/**
 * @brief 联结表达式类 `ConjunctionExpr`，用于将多个表达式使用同一种关系（AND或OR）联结。
 * @ingroup Expression
 * 当前 miniob 仅支持 AND 操作。
 */
class ConjunctionExpr : public Expression
{
public:
  /**
   * @brief 联结类型的枚举。
   */
  enum class Type
  {
    AND,  ///< 与操作
    OR,   ///< 或操作
  };

public:
  /**
   * @brief 构造函数，初始化联结表达式。
   * @param type 联结的类型（AND 或 OR）。
   * @param children 子表达式的向量。
   */
  ConjunctionExpr(Type type, std::vector<std::unique_ptr<Expression>> &children);

  /**
   * @brief 默认析构函数。
   */
  virtual ~ConjunctionExpr() = default;

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::CONJUNCTION`。
   */
  ExprType type() const override { return ExprType::CONJUNCTION; }

  /**
   * @brief 获取表达式的值类型。
   * @return 返回布尔类型 `AttrType::BOOLEANS`。
   */
  AttrType value_type() const override { return AttrType::BOOLEANS; }

  /**
   * @brief 获取联结表达式的值。
   * @param tuple 元组引用。
   * @param[out] value 存储计算结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 获取联结类型。
   * @return 返回联结类型（AND 或 OR）。
   */
  Type conjunction_type() const { return conjunction_type_; }

  /**
   * @brief 获取子表达式的向量。
   * @return 返回子表达式的向量引用。
   */
  std::vector<std::unique_ptr<Expression>> &children() { return children_; }

private:
  Type                                     conjunction_type_;  ///< 联结类型（AND 或 OR）
  std::vector<std::unique_ptr<Expression>> children_;          ///< 子表达式的向量
};

/**
 * @brief 算术表达式类 `ArithmeticExpr`，用于表示算术运算。
 * @ingroup Expression
 */
class ArithmeticExpr : public Expression
{
public:
  /**
   * @brief 算术运算类型的枚举。
   */
  enum class Type
  {
    ADD,       ///< 加法
    SUB,       ///< 减法
    MUL,       ///< 乘法
    DIV,       ///< 除法
    NEGATIVE,  ///< 负数
  };

public:
  /**
   * @brief 构造函数，初始化算术表达式。
   * @param type 算术运算类型。
   * @param left 左操作数表达式。
   * @param right 右操作数表达式。
   */
  ArithmeticExpr(Type type, Expression *left, Expression *right);

  /**
   * @brief 构造函数，使用智能指针初始化算术表达式。
   * @param type 算术运算类型。
   * @param left 左操作数的智能指针。
   * @param right 右操作数的智能指针。
   */
  ArithmeticExpr(Type type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);

  /**
   * @brief 默认析构函数。
   */
  virtual ~ArithmeticExpr() = default;

  /**
   * @brief 判断当前表达式是否与其他表达式相等。
   * @param other 要比较的另一个表达式。
   * @return 若相等返回 `true`，否则返回 `false`。
   */
  bool equal(const Expression &other) const override;

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::ARITHMETIC`。
   */
  ExprType type() const override { return ExprType::ARITHMETIC; }

  /**
   * @brief 获取表达式的值类型。
   * @return 返回运算结果的属性类型。
   */
  AttrType value_type() const override;

  /**
   * @brief 获取表达式值的长度。
   * @return 返回值的长度。如果只有左操作数，返回左操作数的长度；否则返回 4（float 或 int 的大小）。
   */
  int value_length() const override
  {
    if (!right_) {
      return left_->value_length();
    }
    return 4;  // sizeof(float) or sizeof(int)
  };

  /**
   * @brief 获取算术表达式的值。
   * @param tuple 元组引用。
   * @param[out] value 存储计算结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 将算术表达式的值存储到列中。
   * @param chunk 数据块。
   * @param[out] column 存储结果的列。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_column(Chunk &chunk, Column &column) override;

  /**
   * @brief 尝试获取表达式的值，无需依赖元组。
   * @param[out] value 存储运算结果的引用。
   * @return 成功时返回 `RC::SUCCESS`。
   */
  RC try_get_value(Value &value) const override;

  /**
   * @brief 获取算术运算的类型。
   * @return 返回算术运算类型。
   */
  Type arithmetic_type() const { return arithmetic_type_; }

  /**
   * @brief 获取左操作数的智能指针。
   * @return 返回左操作数的智能指针引用。
   */
  std::unique_ptr<Expression> &left() { return left_; }

  /**
   * @brief 获取右操作数的智能指针。
   * @return 返回右操作数的智能指针引用。
   */
  std::unique_ptr<Expression> &right() { return right_; }

private:
  /**
   * @brief 计算两个值的结果。
   * @param left_value 左值。
   * @param right_value 右值。
   * @param[out] value 存储计算结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;

  /**
   * @brief 计算两列的值，并存储结果。
   * @param left_column 左列。
   * @param right_column 右列。
   * @param[out] column 存储结果的列。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC calc_column(const Column &left_column, const Column &right_column, Column &column) const;

  /**
   * @brief 执行计算，根据左右操作数的常量情况选择计算方式。
   * @tparam LEFT_CONSTANT 左操作数是否为常量。
   * @tparam RIGHT_CONSTANT 右操作数是否为常量。
   * @param left 左列。
   * @param right 右列。
   * @param result 存储计算结果的列。
   * @param type 算术运算类型。
   * @param attr_type 属性类型。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
  RC execute_calc(const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const;

private:
  Type                        arithmetic_type_;  ///< 算术运算类型
  std::unique_ptr<Expression> left_;             ///< 左操作数表达式
  std::unique_ptr<Expression> right_;            ///< 右操作数表达式
};

/**
 * @brief 表示未绑定的聚合表达式 `UnboundAggregateExpr`。
 * @ingroup Expression
 */
class UnboundAggregateExpr : public Expression
{
public:
  /**
   * @brief 构造函数，初始化未绑定聚合表达式。
   * @param aggregate_name 聚合函数的名称。
   * @param child 孩子表达式，通常是要聚合的字段。
   */
  UnboundAggregateExpr(const char *aggregate_name, Expression *child);

  /**
   * @brief 默认析构函数。
   */
  virtual ~UnboundAggregateExpr() = default;

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::UNBOUND_AGGREGATION`。
   */
  ExprType type() const override { return ExprType::UNBOUND_AGGREGATION; }

  /**
   * @brief 获取聚合函数的名称。
   * @return 返回聚合函数名称的字符指针。
   */
  const char *aggregate_name() const { return aggregate_name_.c_str(); }

  /**
   * @brief 获取子表达式的智能指针。
   * @return 返回子表达式的智能指针引用。
   */
  std::unique_ptr<Expression> &child() { return child_; }

  /**
   * @brief 获取聚合表达式的值。
   * @param tuple 元组引用。
   * @param[out] value 存储计算结果的引用。
   * @return 返回未实现状态 `RC::INTERNAL`。
   */
  RC get_value(const Tuple &tuple, Value &value) const override { return RC::INTERNAL; }

  /**
   * @brief 获取子表达式的值类型。
   * @return 返回子表达式的属性类型。
   */
  AttrType value_type() const override { return child_->value_type(); }

private:
  std::string                 aggregate_name_;  ///< 聚合函数名称
  std::unique_ptr<Expression> child_;           ///< 子表达式
};

/**
 * @brief 表示聚合表达式 `AggregateExpr`，包括多种聚合函数。
 * @ingroup Expression
 */
class AggregateExpr : public Expression
{
public:
  /**
   * @brief 聚合运算类型的枚举。
   */
  enum class Type
  {
    COUNT,  ///< 计数
    SUM,    ///< 求和
    AVG,    ///< 平均值
    MAX,    ///< 最大值
    MIN,    ///< 最小值
  };

public:
  /**
   * @brief 构造函数，初始化聚合表达式。
   * @param type 聚合函数类型。
   * @param child 子表达式，通常是要聚合的字段。
   */
  AggregateExpr(Type type, Expression *child);

  /**
   * @brief 构造函数，使用智能指针初始化聚合表达式。
   * @param type 聚合函数类型。
   * @param child 子表达式的智能指针。
   */
  AggregateExpr(Type type, std::unique_ptr<Expression> child);

  /**
   * @brief 默认析构函数。
   */
  virtual ~AggregateExpr() = default;

  /**
   * @brief 判断当前表达式是否与其他表达式相等。
   * @param other 要比较的另一个表达式。
   * @return 若相等返回 `true`，否则返回 `false`。
   */
  bool equal(const Expression &other) const override;

  /**
   * @brief 获取表达式的类型。
   * @return 返回表达式类型 `ExprType::AGGREGATION`。
   */
  ExprType type() const override { return ExprType::AGGREGATION; }

  /**
   * @brief 获取子表达式的值类型。
   * @return 返回子表达式的属性类型。
   */
  AttrType value_type() const override { return child_->value_type(); }

  /**
   * @brief 获取子表达式值的长度。
   * @return 返回子表达式值的长度。
   */
  int value_length() const override { return child_->value_length(); }

  /**
   * @brief 获取聚合表达式的值。
   * @param tuple 元组引用。
   * @param[out] value 存储计算结果的引用。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_value(const Tuple &tuple, Value &value) const override;

  /**
   * @brief 将聚合表达式的值存储到列中。
   * @param chunk 数据块。
   * @param[out] column 存储结果的列。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  RC get_column(Chunk &chunk, Column &column) override;

  /**
   * @brief 获取聚合函数的类型。
   * @return 返回聚合函数类型。
   */
  Type aggregate_type() const { return aggregate_type_; }

  /**
   * @brief 获取子表达式的智能指针。
   * @return 返回子表达式的智能指针引用。
   */
  std::unique_ptr<Expression> &child() { return child_; }

  /**
   * @brief 获取子表达式的智能指针（常量）。
   * @return 返回子表达式的智能指针常量引用。
   */
  const std::unique_ptr<Expression> &child() const { return child_; }

  /**
   * @brief 创建聚合器，用于计算聚合结果。
   * @return 返回一个新的聚合器实例。
   */
  std::unique_ptr<Aggregator> create_aggregator() const;

  /**
   * @brief 根据字符串类型转换为聚合类型。
   * @param type_str 输入的字符串。
   * @param[out] type 输出的聚合类型。
   * @return 成功时返回 `RC::SUCCESS`，否则返回相应错误代码。
   */
  static RC type_from_string(const char *type_str, Type &type);

private:
  Type                        aggregate_type_;  ///< 聚合函数类型
  std::unique_ptr<Expression> child_;           ///< 子表达式
};
