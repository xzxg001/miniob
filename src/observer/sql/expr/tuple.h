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
// Created by Wangyunlai on 2021/5/14.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/expr/tuple_cell.h"
#include "sql/parser/parse.h"
#include "common/value.h"
#include "storage/record/record.h"

class Table;

/**
 * @defgroup Tuple
 * @brief Tuple 元组，表示一行数据，当前返回客户端时使用
 * @details
 * tuple是一种可以嵌套的数据结构。
 * 比如select t1.a+t2.b from t1, t2;
 * 需要使用下面的结构表示：
 * @code {.cpp}
 *  Project(t1.a+t2.b)
 *        |
 *      Joined
 *      /     \
 *   Row(t1) Row(t2)
 * @endcode
 * TODO 一个类拆分成一个文件，并放到单独的目录中
 */

/**
 * @brief 元组的结构，包含哪些字段(这里成为Cell)，每个字段的说明
 * @ingroup Tuple
 */
class TupleSchema
{
public:
  /**
   * @brief 向元组模式中添加一个新的单元格描述。
   * @param cell 要添加的单元格描述对象。
   */
  void append_cell(const TupleCellSpec &cell) { cells_.push_back(cell); }  // 将cell对象添加到cells_数组中

  /**
   * @brief 根据表名和字段名创建单元格描述，并将其添加到元组模式中。
   * @param table 表名。
   * @param field 字段名。
   */
  void append_cell(const char *table, const char *field)
  {
    append_cell(TupleCellSpec(table, field));
  }  // 调用重载函数，将表名和字段名作为参数构建单元格描述

  /**
   * @brief 根据别名创建单元格描述，并将其添加到元组模式中。
   * @param alias 别名。
   */
  void append_cell(const char *alias) { append_cell(TupleCellSpec(alias)); }  // 将别名作为参数构建单元格描述

  /**
   * @brief 获取元组模式中单元格的数量。
   * @return 单元格数量。
   */
  int cell_num() const { return static_cast<int>(cells_.size()); }  // 返回cells_数组的大小（单元格的数量）

  /**
   * @brief 获取指定位置的单元格描述。
   * @param i 要获取的单元格索引。
   * @return 指定位置的单元格描述。
   */
  const TupleCellSpec &cell_at(int i) const { return cells_[i]; }  // 获取cells_数组中索引为i的元素（单元格描述）

private:
  std::vector<TupleCellSpec> cells_;  // 存储元组模式中各单元格的描述
};

/**
 * @brief 元组的抽象描述类，用于定义元组的基本行为和接口。
 * @ingroup Tuple
 */
class Tuple
{
public:
  /**
   * @brief 默认构造函数。
   */
  Tuple() = default;

  /**
   * @brief 虚析构函数。
   */
  virtual ~Tuple() = default;

  /**
   * @brief 获取元组中的单元格的数量。
   * @details 单元格数量应与TupleSchema中的定义一致。
   * @return 元组中单元格的数量。
   */
  virtual int cell_num() const = 0;

  /**
   * @brief 获取指定位置的单元格。
   * @param index 要获取的单元格位置索引。
   * @param[out] cell 用于返回获取到的单元格。
   * @return 返回操作状态。
   */
  virtual RC cell_at(int index, Value &cell) const = 0;

  /**
   * @brief 获取指定位置的单元格描述。
   * @param index 要获取的单元格位置索引。
   * @param[out] spec 返回的单元格描述。
   * @return 返回操作状态。
   */
  virtual RC spec_at(int index, TupleCellSpec &spec) const = 0;

  /**
   * @brief 根据单元格的描述，获取单元格的值。
   * @param spec 单元格描述信息。
   * @param[out] cell 返回获取到的单元格值。
   * @return 返回操作状态。
   */
  virtual RC find_cell(const TupleCellSpec &spec, Value &cell) const = 0;

  /**
   * @brief 将元组转换为字符串表示。
   * @return 元组的字符串表示。
   */
  virtual std::string to_string() const
  {
    std::string str;                          // 用于存储最终的字符串表示
    const int   cell_num = this->cell_num();  // 获取元组的单元格数量
    for (int i = 0; i < cell_num - 1; i++) {  // 遍历每个单元格
      Value cell;
      cell_at(i, cell);         // 获取第i个单元格
      str += cell.to_string();  // 将单元格值转换为字符串并添加到str中
      str += ", ";              // 添加逗号分隔符
    }

    if (cell_num > 0) {  // 如果元组中至少有一个单元格
      Value cell;
      cell_at(cell_num - 1, cell);  // 获取最后一个单元格
      str += cell.to_string();      // 将最后一个单元格值添加到str中
    }
    return str;  // 返回字符串表示
  }

  /**
   * @brief 比较当前元组与另一个元组。
   * @param other 要比较的另一个元组。
   * @param[out] result 比较结果：-1表示当前元组小于other，0表示相等，1表示大于other。
   * @return 返回操作状态。
   */
  virtual RC compare(const Tuple &other, int &result) const
  {
    RC rc = RC::SUCCESS;  // 初始化操作状态

    const int this_cell_num  = this->cell_num();  // 获取当前元组的单元格数量
    const int other_cell_num = other.cell_num();  // 获取另一个元组的单元格数量
    if (this_cell_num < other_cell_num) {         // 如果当前元组单元格数量小于另一个元组
      result = -1;
      return rc;  // 返回结果 -1 并退出
    }
    if (this_cell_num > other_cell_num) {  // 如果当前元组单元格数量大于另一个元组
      result = 1;
      return rc;  // 返回结果 1 并退出
    }

    Value this_value;                          // 用于存储当前元组的单元格值
    Value other_value;                         // 用于存储另一个元组的单元格值
    for (int i = 0; i < this_cell_num; i++) {  // 遍历每个单元格
      rc = this->cell_at(i, this_value);       // 获取当前元组第i个单元格的值
      if (OB_FAIL(rc)) {                       // 检查操作状态
        return rc;                             // 如果获取失败，则返回错误状态
      }

      rc = other.cell_at(i, other_value);  // 获取另一个元组第i个单元格的值
      if (OB_FAIL(rc)) {                   // 检查操作状态
        return rc;                         // 如果获取失败，则返回错误状态
      }

      result = this_value.compare(other_value);  // 比较两个单元格的值
      if (0 != result) {                         // 如果单元格值不相等
        return rc;                               // 返回当前状态，退出
      }
    }

    result = 0;  // 如果所有单元格相等，则将结果设置为 0
    return rc;   // 返回操作状态
  }
};

/**
 * @brief 一行数据的元组
 * @ingroup Tuple
 * @details 直接就是获取表中的一条记录
 */
class RowTuple : public Tuple
{
public:
  /**
   * @brief 默认构造函数。
   */
  RowTuple() = default;

  /**
   * @brief 析构函数。释放 speces_ 向量中的 FieldExpr 对象，防止内存泄漏。
   */
  virtual ~RowTuple()
  {
    for (FieldExpr *spec : speces_) {  // 遍历 speces_ 向量中的每个 FieldExpr 指针
      delete spec;                     // 删除每个 FieldExpr 对象
    }
    speces_.clear();  // 清空 speces_ 向量
  }

  /**
   * @brief 设置记录数据。
   * @param record 指向要设置的记录对象的指针。
   */
  void set_record(Record *record) { this->record_ = record; }  // 将 record_ 指向传入的记录对象

  /**
   * @brief 设置元组的模式，包括表和字段的元数据信息。
   * @param table 指向表对象的指针。
   * @param fields 表中包含的字段的元数据信息的向量。
   */
  void set_schema(const Table *table, const std::vector<FieldMeta> *fields)
  {
    table_ = table;  // 设置表对象
    // fix: join操作时右表会多次调用open，并调用set_schema，因此需先清空 speces_，避免重复数据
    for (FieldExpr *spec : speces_) {  // 遍历 speces_ 向量，清理旧的 FieldExpr 对象
      delete spec;
    }
    this->speces_.clear();                              // 清空 speces_ 向量
    this->speces_.reserve(fields->size());              // 预留 fields 大小的存储空间
    for (const FieldMeta &field : *fields) {            // 遍历字段元数据信息
      speces_.push_back(new FieldExpr(table, &field));  // 为每个字段创建 FieldExpr 对象并添加到 speces_ 向量
    }
  }

  /**
   * @brief 获取元组中单元格的数量。
   * @return 返回元组中单元格的数量。
   */
  int cell_num() const override { return speces_.size(); }  // 返回 speces_ 向量中的元素数量，即单元格的数量

  /**
   * @brief 获取指定索引位置的单元格值。
   * @param index 要获取的单元格的索引位置。
   * @param[out] cell 用于返回获取到的单元格值。
   * @return 返回操作状态。
   */
  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {  // 检查索引范围
      LOG_WARN("invalid argument. index=%d", index);                // 记录无效索引警告日志
      return RC::INVALID_ARGUMENT;                                 // 返回无效参数状态
    }

    FieldExpr       *field_expr = speces_[index];              // 获取指定位置的 FieldExpr 对象
    const FieldMeta *field_meta = field_expr->field().meta();  // 获取字段的元数据信息
    cell.set_type(field_meta->type());                         // 设置单元格类型
    cell.set_data(this->record_->data() + field_meta->offset(), field_meta->len());  // 设置单元格的数据和长度
    return RC::SUCCESS;                                                              // 返回成功状态
  }

  /**
   * @brief 获取指定索引位置的单元格描述信息。
   * @param index 要获取的单元格索引位置。
   * @param[out] spec 用于返回的单元格描述。
   * @return 返回操作状态。
   */
  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    const Field &field = speces_[index]->field();                            // 获取指定位置的 Field 对象
    spec               = TupleCellSpec(table_->name(), field.field_name());  // 设置单元格描述
    return RC::SUCCESS;                                                      // 返回成功状态
  }

  /**
   * @brief 根据单元格描述查找并返回单元格值。
   * @param spec 单元格描述，包括表名和字段名。
   * @param[out] cell 返回查找到的单元格值。
   * @return 返回操作状态，如果未找到则返回 RC::NOTFOUND。
   */
  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    const char *table_name = spec.table_name();     // 获取描述中的表名
    const char *field_name = spec.field_name();     // 获取描述中的字段名
    if (0 != strcmp(table_name, table_->name())) {  // 比较表名，确保一致性
      return RC::NOTFOUND;                          // 表名不一致，返回未找到状态
    }

    for (size_t i = 0; i < speces_.size(); ++i) {         // 遍历所有字段表达式
      const FieldExpr *field_expr = speces_[i];           // 获取每个字段表达式
      const Field     &field      = field_expr->field();  // 获取字段对象
      if (0 == strcmp(field_name, field.field_name())) {  // 检查字段名是否匹配
        return cell_at(i, cell);                          // 找到匹配字段，返回对应单元格值
      }
    }
    return RC::NOTFOUND;  // 没有找到匹配字段，返回未找到状态
  }

#if 0
  /**
   * @brief 获取指定索引位置的单元格描述指针。
   * @param index 要获取的单元格描述的索引位置。
   * @param[out] spec 返回的单元格描述指针。
   * @return 返回操作状态。
   */
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) { // 检查索引范围
      LOG_WARN("invalid argument. index=%d", index); // 记录无效索引警告日志
      return RC::INVALID_ARGUMENT; // 返回无效参数状态
    }
    spec = speces_[index]; // 设置 spec 指向指定索引位置的单元格描述
    return RC::SUCCESS; // 返回成功状态
  }
#endif

  /**
   * @brief 获取元组中的记录。
   * @return 返回元组中的记录。
   */
  Record &record() { return *record_; }  // 返回指向 record_ 的引用

  /**
   * @brief 获取元组中的记录（只读）。
   * @return 返回元组中的记录的常量引用。
   */
  const Record &record() const { return *record_; }  // 返回指向 record_ 的常量引用

private:
  Record                  *record_ = nullptr;  // 指向元组中记录的指针
  const Table             *table_  = nullptr;  // 指向元组所属的表的指针
  std::vector<FieldExpr *> speces_;            // 存储字段表达式的向量，用于描述元组模式
};

/**
 * @brief `ProjectTuple` 类用于从一行数据中选择部分字段生成一个新元组，实现了投影操作。
 * @ingroup Tuple
 * @details 一般在 SQL 的 SELECT 语句中使用。投影操作允许在返回的结果集中只包含部分列，
 * 并可在这些列上执行类型转换、重命名、表达式计算、函数应用等操作。
 */
class ProjectTuple : public Tuple
{
public:
  /**
   * @brief 默认构造函数。
   */
  ProjectTuple() = default;

  /**
   * @brief 默认析构函数。
   */
  virtual ~ProjectTuple() = default;

  /**
   * @brief 设置表达式列表，用于描述选择出的字段及其计算逻辑。
   * @param expressions 包含多个表达式的向量，使用右值引用用于移动语义。
   */
  void set_expressions(std::vector<std::unique_ptr<Expression>> &&expressions)
  {
    expressions_ = std::move(expressions);  // 将传入的表达式向量移动到成员变量 expressions_ 中
  }

  /**
   * @brief 获取表达式列表。
   * @return 返回表达式列表的常量引用。
   */
  auto get_expressions() const -> const std::vector<std::unique_ptr<Expression>> & { return expressions_; }

  /**
   * @brief 设置基础元组，用于从中提取需要的字段。
   * @param tuple 指向基础元组的指针。
   */
  void set_tuple(Tuple *tuple) { this->tuple_ = tuple; }

  /**
   * @brief 获取元组中单元格的数量，即投影后的字段数量。
   * @return 返回表达式数量，即该元组的单元格数量。
   */
  int cell_num() const override { return static_cast<int>(expressions_.size()); }

  /**
   * @brief 获取指定位置的单元格值。
   * @param index 要获取的单元格的索引位置。
   * @param[out] cell 用于返回的单元格值。
   * @return 返回操作状态。
   */
  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {  // 检查索引范围是否合法
      return RC::INTERNAL;                   // 返回内部错误状态
    }
    if (tuple_ == nullptr) {  // 检查 tuple_ 是否为空
      return RC::INTERNAL;    // 返回内部错误状态
    }

    Expression *expr = expressions_[index].get();  // 获取指定位置的表达式指针
    return expr->get_value(*tuple_, cell);         // 使用表达式从基础元组中计算出指定单元格的值
  }

  /**
   * @brief 获取指定索引位置的单元格描述。
   * @param index 要获取描述的单元格索引。
   * @param[out] spec 用于返回的单元格描述。
   * @return 返回操作状态。
   */
  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    spec = TupleCellSpec(expressions_[index]->name());  // 使用表达式的名称创建单元格描述
    return RC::SUCCESS;                                 // 返回成功状态
  }

  /**
   * @brief 根据单元格描述查找并返回单元格值。
   * @param spec 单元格描述，包括名称信息。
   * @param[out] cell 用于返回查找到的单元格值。
   * @return 返回操作状态。
   */
  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    return tuple_->find_cell(spec, cell);  // 在基础元组中根据描述查找单元格
  }

#if 0
  /**
   * @brief 获取指定索引位置的单元格描述指针（此功能已关闭）。
   * @param index 要获取描述的单元格索引。
   * @param[out] spec 用于返回的单元格描述指针。
   * @return 返回操作状态。
   */
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) { // 检查索引范围是否合法
      return RC::NOTFOUND; // 返回未找到状态
    }
    spec = speces_[index]; // 设置 spec 指向指定索引位置的单元格描述
    return RC::SUCCESS; // 返回成功状态
  }
#endif

private:
  std::vector<std::unique_ptr<Expression>> expressions_;      // 保存投影操作的表达式列表
  Tuple                                   *tuple_ = nullptr;  // 指向基础元组的指针，用于执行投影操作
};

/**
 * @brief `ValueListTuple` 类表示一个由常量值组成的 Tuple 元组。
 * @ingroup Tuple
 * @details 该类包含一组不随数据表变化的固定常量值，用于返回结果中的特定常量列或表达式值。
 * TODO: 建议将该类移到一个单独的文件中，便于代码管理。
 */
class ValueListTuple : public Tuple
{
public:
  /**
   * @brief 默认构造函数。
   */
  ValueListTuple() = default;

  /**
   * @brief 默认析构函数。
   */
  virtual ~ValueListTuple() = default;

  /**
   * @brief 设置元组的列名信息。
   * @param specs 传入的 `TupleCellSpec` 向量，表示各列的元信息。
   */
  void set_names(const std::vector<TupleCellSpec> &specs) { specs_ = specs; }

  /**
   * @brief 设置元组的值数据。
   * @param cells 包含各个列值的 `Value` 向量。
   */
  void set_cells(const std::vector<Value> &cells) { cells_ = cells; }

  /**
   * @brief 获取元组中单元格的数量。
   * @return 返回单元格数量。
   */
  virtual int cell_num() const override { return static_cast<int>(cells_.size()); }

  /**
   * @brief 获取指定索引的单元格值。
   * @param index 要获取的单元格索引。
   * @param[out] cell 用于返回的单元格值。
   * @return 成功时返回 `RC::SUCCESS`，否则返回 `RC::NOTFOUND`。
   */
  virtual RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {  // 检查索引是否有效
      return RC::NOTFOUND;
    }

    cell = cells_[index];  // 返回指定索引位置的值
    return RC::SUCCESS;
  }

  /**
   * @brief 获取指定索引的单元格描述信息。
   * @param index 单元格的索引。
   * @param[out] spec 用于返回的单元格描述。
   * @return 成功时返回 `RC::SUCCESS`，否则返回 `RC::NOTFOUND`。
   */
  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    if (index < 0 || index >= cell_num()) {  // 检查索引是否有效
      return RC::NOTFOUND;
    }

    spec = specs_[index];  // 返回指定索引位置的描述
    return RC::SUCCESS;
  }

  /**
   * @brief 根据描述信息查找单元格值。
   * @param spec 单元格描述信息，包括名称信息。
   * @param[out] cell 用于返回的找到的单元格值。
   * @return 成功时返回 `RC::SUCCESS`，如果未找到则返回 `RC::NOTFOUND`。
   */
  virtual RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    ASSERT(cells_.size() == specs_.size(), "cells_.size()=%d, specs_.size()=%d", cells_.size(), specs_.size());

    const int size = static_cast<int>(specs_.size());
    for (int i = 0; i < size; i++) {  // 遍历 specs_ 找到匹配的描述信息
      if (specs_[i].equals(spec)) {   // 如果找到匹配的描述信息
        cell = cells_[i];             // 返回对应的值
        return RC::SUCCESS;
      }
    }
    return RC::NOTFOUND;  // 如果没有找到匹配项，返回未找到
  }

  /**
   * @brief 从给定的元组生成一个 `ValueListTuple` 对象，复制该元组的值和描述信息。
   * @param tuple 输入元组，提供需要复制的单元格值和描述。
   * @param[out] value_list 用于存储生成的 `ValueListTuple` 对象。
   * @return 成功时返回 `RC::SUCCESS`。
   */
  static RC make(const Tuple &tuple, ValueListTuple &value_list)
  {
    const int cell_num = tuple.cell_num();
    for (int i = 0; i < cell_num; i++) {
      Value cell;
      RC    rc = tuple.cell_at(i, cell);  // 从输入元组中获取单元格值
      if (OB_FAIL(rc)) {
        return rc;  // 如果获取失败，返回错误状态
      }

      TupleCellSpec spec;
      rc = tuple.spec_at(i, spec);  // 从输入元组中获取单元格描述
      if (OB_FAIL(rc)) {
        return rc;  // 如果获取失败，返回错误状态
      }

      value_list.cells_.push_back(cell);  // 将值存储到 value_list 中
      value_list.specs_.push_back(spec);  // 将描述存储到 value_list 中
    }
    return RC::SUCCESS;  // 返回成功状态
  }

private:
  std::vector<Value>         cells_;  // 存储元组的单元格值
  std::vector<TupleCellSpec> specs_;  // 存储元组的单元格描述信息
};

/**
 * @brief `JoinedTuple` 类用于将两个 `Tuple` 对象合并为一个新 `Tuple`。
 * @ingroup Tuple
 * @details 主要用于 `join` 算子中，通过包含左、右两个 `Tuple` 实例来提供对连接后元组的访问。
 * TODO: 计划替换为复合 `Tuple`，以提高代码的通用性。
 */
class JoinedTuple : public Tuple
{
public:
  /**
   * @brief 默认构造函数。
   */
  JoinedTuple() = default;

  /**
   * @brief 默认析构函数。
   */
  virtual ~JoinedTuple() = default;

  /**
   * @brief 设置左侧的 `Tuple` 对象。
   * @param left 左侧元组指针。
   */
  void set_left(Tuple *left) { left_ = left; }

  /**
   * @brief 设置右侧的 `Tuple` 对象。
   * @param right 右侧元组指针。
   */
  void set_right(Tuple *right) { right_ = right; }

  /**
   * @brief 获取合并后的元组中单元格的总数量。
   * @return 返回左侧元组和右侧元组单元格数量的总和。
   */
  int cell_num() const override { return left_->cell_num() + right_->cell_num(); }

  /**
   * @brief 获取合并元组中指定索引的单元格值。
   * @param index 要获取的单元格索引。
   * @param[out] value 用于返回的单元格值。
   * @return 成功时返回 `RC::SUCCESS`，如果索引无效则返回 `RC::NOTFOUND`。
   */
  RC cell_at(int index, Value &value) const override
  {
    const int left_cell_num = left_->cell_num();  // 左侧元组的单元格数量
    if (index >= 0 && index < left_cell_num) {    // 索引在左侧元组范围内
      return left_->cell_at(index, value);        // 从左侧元组获取单元格值
    }

    if (index >= left_cell_num && index < left_cell_num + right_->cell_num()) {  // 索引在右侧元组范围内
      return right_->cell_at(index - left_cell_num, value);                      // 从右侧元组获取单元格值
    }

    return RC::NOTFOUND;  // 索引无效
  }

  /**
   * @brief 获取合并元组中指定索引的单元格描述信息。
   * @param index 单元格的索引。
   * @param[out] spec 用于返回的单元格描述。
   * @return 成功时返回 `RC::SUCCESS`，如果索引无效则返回 `RC::NOTFOUND`。
   */
  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    const int left_cell_num = left_->cell_num();  // 左侧元组的单元格数量
    if (index >= 0 && index < left_cell_num) {    // 索引在左侧元组范围内
      return left_->spec_at(index, spec);         // 从左侧元组获取单元格描述信息
    }

    if (index >= left_cell_num && index < left_cell_num + right_->cell_num()) {  // 索引在右侧元组范围内
      return right_->spec_at(index - left_cell_num, spec);  // 从右侧元组获取单元格描述信息
    }

    return RC::NOTFOUND;  // 索引无效
  }

  /**
   * @brief 根据描述信息查找单元格值。
   * @param spec 单元格描述信息。
   * @param[out] value 用于返回的找到的单元格值。
   * @return 成功时返回 `RC::SUCCESS`，如果未找到则返回 `RC::NOTFOUND`。
   */
  RC find_cell(const TupleCellSpec &spec, Value &value) const override
  {
    RC rc = left_->find_cell(spec, value);          // 先在左侧元组中查找单元格
    if (rc == RC::SUCCESS || rc != RC::NOTFOUND) {  // 如果找到或出现其他错误
      return rc;
    }

    return right_->find_cell(spec, value);  // 未在左侧找到，继续在右侧查找单元格
  }

private:
  Tuple *left_  = nullptr;  // 左侧元组
  Tuple *right_ = nullptr;  // 右侧元组
};
