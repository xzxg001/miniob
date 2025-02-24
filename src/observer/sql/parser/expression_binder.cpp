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
// Created by Wangyunlai on 2024/05/29.
//

#include <algorithm>  // 包含STL算法库

#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/parser/expression_binder.h"
#include "sql/expr/expression_iterator.h"

using namespace std;
using namespace common;

// 在查询中查找具有给定名称的表对象
Table *BinderContext::find_table(const char *table_name) const
{
  // 使用C++14的lambda表达式定义查找谓词
  auto pred = [table_name](Table *table) { return 0 == strcasecmp(table_name, table->name()); };
  // 使用STL的find_if算法在query_tables_中查找第一个满足谓词的表对象
  auto iter = ranges::find_if(query_tables_, pred);
  if (iter == query_tables_.end()) {
    return nullptr;  // 如果没有找到，返回nullptr
  }
  return *iter;  // 返回找到的表对象
}

////////////////////////////////////////////////////////////////////////////////
// 用于处理SQL语句中的通配符字段，为表中的每个字段创建一个表达式
static void wildcard_fields(Table *table, vector<unique_ptr<Expression>> &expressions)
{
  const TableMeta &table_meta = table->table_meta();  // 获取表的元数据
  const int field_num = table_meta.field_num();  // 获取字段总数
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    Field field(table, table_meta.field(i));  // 创建字段对象
    FieldExpr *field_expr = new FieldExpr(field);  // 创建字段表达式
    field_expr->set_name(field.field_name());  // 设置字段表达式的名称
    expressions.emplace_back(field_expr);  // 将字段表达式添加到列表中
  }
}

// 将表达式绑定到具体的字段
RC ExpressionBinder::bind_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  // 根据表达式的类型进行不同的绑定操作
  switch (expr->type()) {
    case ExprType::STAR: {
      return bind_star_expression(expr, bound_expressions);  // 绑定星号表达式
    } break;

    case ExprType::UNBOUND_FIELD: {
      return bind_unbound_field_expression(expr, bound_expressions);  // 绑定未绑定的字段表达式
    } break;

    case ExprType::UNBOUND_AGGREGATION: {
      return bind_aggregate_expression(expr, bound_expressions);  // 绑定聚合表达式
    } break;

    case ExprType::FIELD: {
      return bind_field_expression(expr, bound_expressions);  // 绑定字段表达式
    } break;

    case ExprType::VALUE: {
      return bind_value_expression(expr, bound_expressions);  // 绑定值表达式
    } break;

    case ExprType::CAST: {
      return bind_cast_expression(expr, bound_expressions);  // 绑定类型转换表达式
    } break;

    case ExprType::COMPARISON: {
      return bind_comparison_expression(expr, bound_expressions);  // 绑定比较表达式
    } break;

    case ExprType::CONJUNCTION: {
      return bind_conjunction_expression(expr, bound_expressions);  // 绑定逻辑连接表达式
    } break;

    case ExprType::ARITHMETIC: {
      return bind_arithmetic_expression(expr, bound_expressions);  // 绑定算术表达式
    } break;

    case ExprType::AGGREGATION: {
      ASSERT(false, "shouldn't be here");  // 聚合表达式不应该在这里处理
    } break;

    default: {
      LOG_WARN("unknown expression type: %d", static_cast<int>(expr->type()));  // 记录未知表达式类型的警告
      return RC::INTERNAL;  // 返回内部错误
    }
  }
  return RC::INTERNAL;  // 默认返回内部错误
}
RC ExpressionBinder::bind_star_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto star_expr = static_cast<StarExpr *>(expr.get());  // 将表达式转换为星号表达式类型

  vector<Table *> tables_to_wildcard;  // 存储需要应用通配符的表对象

  const char *table_name = star_expr->table_name();  // 获取星号表达式指定的表名
  if (!is_blank(table_name) && 0 != strcmp(table_name, "*")) {
    Table *table = context_.find_table(table_name);  // 在查询中查找表对象
    if (nullptr == table) {
      LOG_INFO("no such table in from list: %s", table_name);  // 如果表不存在，记录日志
      return RC::SCHEMA_TABLE_NOT_EXIST;  // 返回表不存在错误码
    }

    tables_to_wildcard.push_back(table);  // 将找到的表添加到列表中
  } else {
    const vector<Table *> &all_tables = context_.query_tables();  // 获取查询中所有表对象
    tables_to_wildcard.insert(tables_to_wildcard.end(), all_tables.begin(), all_tables.end());  // 添加所有表到列表
  }

  for (Table *table : tables_to_wildcard) {
    wildcard_fields(table, bound_expressions);  // 为每个表应用通配符字段处理
  }

  return RC::SUCCESS;  // 返回成功状态码
}
RC ExpressionBinder::bind_unbound_field_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto unbound_field_expr = static_cast<UnboundFieldExpr *>(expr.get());  // 将表达式转换为未绑定字段表达式类型

  const char *table_name = unbound_field_expr->table_name();  // 获取字段表达式指定的表名
  const char *field_name = unbound_field_expr->field_name();  // 获取字段表达式指定的字段名

  Table *table = nullptr;  // 初始化表对象为nullptr
  if (is_blank(table_name)) {
    if (context_.query_tables().size() != 1) {
      LOG_INFO("cannot determine table for field: %s", field_name);  // 如果无法确定字段的表，记录日志
      return RC::SCHEMA_TABLE_NOT_EXIST;  // 返回表不存在错误码
    }

    table = context_.query_tables()[0];  // 如果查询中只有一个表，直接使用该表
  } else {
    table = context_.find_table(table_name);  // 在查询中查找表对象
    if (nullptr == table) {
      LOG_INFO("no such table in from list: %s", table_name);  // 如果表不存在，记录日志
      return RC::SCHEMA_TABLE_NOT_EXIST;  // 返回表不存在错误码
    }
  }

  if (0 == strcmp(field_name, "*")) {
    wildcard_fields(table, bound_expressions);  // 如果字段名为星号，应用通配符字段处理
  } else {
    const FieldMeta *field_meta = table->table_meta().field(field_name);  // 获取字段的元数据
    if (nullptr == field_meta) {
      LOG_INFO("no such field in table: %s.%s", table_name, field_name);  // 如果字段不存在，记录日志
      return RC::SCHEMA_FIELD_MISSING;  // 返回字段缺失错误码
    }

    Field field(table, field_meta);  // 创建字段对象
    FieldExpr *field_expr = new FieldExpr(field);  // 创建字段表达式
    field_expr->set_name(field_name);  // 设置字段表达式的名称
    bound_expressions.emplace_back(field_expr);  // 将字段表达式添加到列表中
  }

  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_field_expression(
    unique_ptr<Expression> &field_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(field_expr));  // 将字段表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_value_expression(
    unique_ptr<Expression> &value_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(value_expr));  // 将值表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_cast_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto cast_expr = static_cast<CastExpr *>(expr.get());  // 将表达式转换为类型转换表达式类型

  vector<unique_ptr<Expression>> child_bound_expressions;  // 存储子表达式绑定结果
  unique_ptr<Expression> &child_expr = cast_expr->child();  // 获取子表达式

  RC rc = bind_expression(child_expr, child_bound_expressions);  // 绑定子表达式
  if (rc != RC::SUCCESS) {
    return rc;  // 如果绑定失败，返回错误码
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid children number of cast expression: %d", child_bound_expressions.size());  // 记录日志
    return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
  }

  unique_ptr<Expression> &child = child_bound_expressions[0];  // 获取绑定后的子表达式
  if (child.get() == child_expr.get()) {
    return RC::SUCCESS;  // 如果子表达式没有变化，返回成功
  }

  child_expr.reset(child.release());  // 更新子表达式
  bound_expressions.emplace_back(std::move(expr));  // 将类型转换表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_comparison_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());  // 将表达式转换为比较表达式类型

  vector<unique_ptr<Expression>> child_bound_expressions;  // 存储子表达式绑定结果
  unique_ptr<Expression> &left_expr = comparison_expr->left();  // 获取左子表达式
  unique_ptr<Expression> &right_expr = comparison_expr->right();  // 获取右子表达式

  RC rc = bind_expression(left_expr, child_bound_expressions);  // 绑定左子表达式
  if (rc != RC::SUCCESS) {
    return rc;  // 如果绑定失败，返回错误码
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());  // 记录日志
    return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];  // 获取绑定后的左子表达式
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());  // 更新左子表达式
  }

  child_bound_expressions.clear();  // 清空子表达式绑定结果
  rc = bind_expression(right_expr, child_bound_expressions);  // 绑定右子表达式
  if (rc != RC::SUCCESS) {
    return rc;  // 如果绑定失败，返回错误码
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());  // 记录日志
    return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];  // 获取绑定后的右子表达式
  if (right.get() != right_expr.get()) {
    right_expr.reset(right.release());  // 更新右子表达式
  }

  bound_expressions.emplace_back(std::move(expr));  // 将比较表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_conjunction_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());  // 将表达式转换为逻辑连接表达式类型

  vector<unique_ptr<Expression>> child_bound_expressions;  // 存储子表达式绑定结果
  vector<unique_ptr<Expression>> &children = conjunction_expr->children();  // 获取子表达式列表

  for (unique_ptr<Expression> &child_expr : children) {
    child_bound_expressions.clear();  // 清空子表达式绑定结果

    RC rc = bind_expression(child_expr, child_bound_expressions);  // 绑定子表达式
    if (rc != RC::SUCCESS) {
      return rc;  // 如果绑定失败，返回错误码
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid children number of conjunction expression: %d", child_bound_expressions.size());  // 记录日志
      return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
    }

    unique_ptr<Expression> &child = child_bound_expressions[0];  // 获取绑定后的子表达式
    if (child.get() != child_expr.get()) {
      child_expr.reset(child.release());  // 更新子表达式
    }
  }

  bound_expressions.emplace_back(std::move(expr));  // 将逻辑连接表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC ExpressionBinder::bind_arithmetic_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;  // 如果表达式为空，直接返回成功
  }

  auto arithmetic_expr = static_cast<ArithmeticExpr *>(expr.get());  // 将表达式转换为算术表达式类型

  vector<unique_ptr<Expression>> child_bound_expressions;  // 存储子表达式绑定结果
  unique_ptr<Expression> &left_expr = arithmetic_expr->left();  // 获取左子表达式
  unique_ptr<Expression> &right_expr = arithmetic_expr->right();  // 获取右子表达式

  RC rc = bind_expression(left_expr, child_bound_expressions);  // 绑定左子表达式
  if (OB_FAIL(rc)) {  // 如果返回码表示失败，返回错误码
    return rc;
  }

  if (child_bound_expressions.size() != 1) {  // 确保只有一个绑定的左子表达式
    LOG_WARN("invalid left children number of arithmetic expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];  // 获取绑定后的左子表达式
  if (left.get() != left_expr.get()) {  // 如果左子表达式发生变化，更新原表达式
    left_expr.reset(left.release());
  }

  child_bound_expressions.clear();  // 清空子表达式绑定结果
  rc = bind_expression(right_expr, child_bound_expressions);  // 绑定右子表达式
  if (OB_FAIL(rc)) {  // 如果返回码表示失败，返回错误码
    return rc;
  }

  if (child_bound_expressions.size() != 1) {  // 确保只有一个绑定的右子表达式
    LOG_WARN("invalid right children number of arithmetic expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;  // 返回参数无效错误码
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];  // 获取绑定后的右子表达式
  if (right.get() != right_expr.get()) {  // 如果右子表达式发生变化，更新原表达式
    right_expr.reset(right.release());
  }

  bound_expressions.emplace_back(std::move(expr));  // 将算术表达式移动到绑定表达式列表
  return RC::SUCCESS;  // 返回成功状态码
}

RC check_aggregate_expression(AggregateExpr &expression)
{
  // 必须有一个子表达式
  Expression *child_expression = expression.child().get();
  if (nullptr == child_expression) {
    LOG_WARN("child expression of aggregate expression is null");
    return RC::INVALID_ARGUMENT;  // 如果子表达式为空，返回参数无效错误码
  }

  // 校验数据类型与聚合类型是否匹配
  AggregateExpr::Type aggregate_type = expression.aggregate_type();  // 获取聚合类型
  AttrType child_value_type = child_expression->value_type();  // 获取子表达式的值类型
  switch (aggregate_type) {
    case AggregateExpr::Type::SUM:
    case AggregateExpr::Type::AVG: {
      // 仅支持数值类型
      if (child_value_type != AttrType::INTS && child_value_type != AttrType::FLOATS) {
        LOG_WARN("invalid child value type for aggregate expression: %d", static_cast<int>(child_value_type));
        return RC::INVALID_ARGUMENT;  // 如果类型不匹配，返回参数无效错误码
      }
    } break;

    case AggregateExpr::Type::COUNT:
    case AggregateExpr::Type::MAX:
    case AggregateExpr::Type::MIN: {
      // 任何类型都支持
    } break;
  }

  // 子表达式中不能再包含聚合表达式
  function<RC(std::unique_ptr<Expression>&)> check_aggregate_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      LOG_WARN("aggregate expression cannot be nested");
      return RC::INVALID_ARGUMENT;  // 如果子表达式是聚合表达式，返回参数无效错误码
    }
    rc = ExpressionIterator::iterate_child_expr(expr, check_aggregate_expr);  // 递归检查子表达式
    return rc;
  };

  RC rc = ExpressionIterator::iterate_child_expr(expression, check_aggregate_expr);  // 开始递归检查

  return rc;  // 返回检查结果
}

RC ExpressionBinder::bind_aggregate_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  // 如果表达式为空，返回成功状态码
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  // 将表达式转换为未绑定的聚合表达式类型
  auto unbound_aggregate_expr = static_cast<UnboundAggregateExpr *>(expr.get());
  
  // 获取聚合函数的名称
  const char *aggregate_name = unbound_aggregate_expr->aggregate_name();
  
  // 定义一个变量来存储解析后的聚合类型
  AggregateExpr::Type aggregate_type;
  
  // 根据聚合函数的名称解析为对应的枚举类型
  RC rc = AggregateExpr::type_from_string(aggregate_name, aggregate_type);
  // 如果解析失败，记录警告日志并返回错误码
  if (OB_FAIL(rc)) {
    LOG_WARN("invalid aggregate name: %s", aggregate_name);
    return rc;
  }

  // 获取聚合表达式的子表达式
  unique_ptr<Expression> &child_expr = unbound_aggregate_expr->child();
  // 定义一个向量来存储子表达式的绑定结果
  vector<unique_ptr<Expression>> child_bound_expressions;

  // 如果子表达式是星号(*)并且聚合类型是计数(COUNT)，则将子表达式替换为值1
  if (child_expr->type() == ExprType::STAR && aggregate_type == AggregateExpr::Type::COUNT) {
    ValueExpr *value_expr = new ValueExpr(Value(1));
    child_expr.reset(value_expr);
  } else {
    // 否则，对子表达式进行绑定
    rc = bind_expression(child_expr, child_bound_expressions);
    // 如果绑定失败，返回错误码
    if (OB_FAIL(rc)) {
      return rc;
    }

    // 确保只有一个绑定的子表达式
    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid children number of aggregate expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    // 如果子表达式发生变化，更新原表达式
    if (child_bound_expressions[0].get() != child_expr.get()) {
      child_expr.reset(child_bound_expressions[0].release());
    }
  }

  // 创建一个新的聚合表达式对象，设置聚合类型和子表达式
  auto aggregate_expr = make_unique<AggregateExpr>(aggregate_type, std::move(child_expr));
  // 设置聚合表达式的名字
  aggregate_expr->set_name(unbound_aggregate_expr->name());
  
  // 验证聚合表达式的合法性
  rc = check_aggregate_expression(*aggregate_expr);
  // 如果验证失败，返回错误码
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 将聚合表达式添加到绑定表达式列表
  bound_expressions.emplace_back(std::move(aggregate_expr));
  
  // 返回成功状态码
  return RC::SUCCESS;
}
