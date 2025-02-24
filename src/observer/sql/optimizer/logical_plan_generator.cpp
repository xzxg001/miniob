/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//
#include "sql/optimizer/logical_plan_generator.h"  // 包含逻辑计划生成器的头文件

#include <common/log/log.h>  // 包含日志记录的头文件

// 包含各种逻辑操作符的头文件
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/group_by_logical_operator.h"

// 包含各种SQL语句的头文件
#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"

#include "sql/expr/expression_iterator.h"  // 包含表达式迭代器的头文件

using namespace std;  // 使用标准命名空间
using namespace common;  // 使用common命名空间

// create函数用于根据SQL语句生成逻辑操作符
RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功
  // 根据语句类型进行不同的处理
  switch (stmt->type()) {
    case StmtType::CALC: {  // 计算语句
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);  // 转换为CalcStmt类型

      rc = create_plan(calc_stmt, logical_operator);  // 创建计算语句的逻辑计划
    } break;

    case StmtType::SELECT: {  // 选择语句
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);  // 转换为SelectStmt类型

      rc = create_plan(select_stmt, logical_operator);  // 创建选择语句的逻辑计划
    } break;

    case StmtType::INSERT: {  // 插入语句
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);  // 转换为InsertStmt类型

      rc = create_plan(insert_stmt, logical_operator);  // 创建插入语句的逻辑计划
    } break;

    case StmtType::DELETE: {  // 删除语句
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);  // 转换为DeleteStmt类型

      rc = create_plan(delete_stmt, logical_operator);  // 创建删除语句的逻辑计划
    } break;

    case StmtType::EXPLAIN: {  // 解释语句
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);  // 转换为ExplainStmt类型

      rc = create_plan(explain_stmt, logical_operator);  // 创建解释语句的逻辑计划
    } break;
    default: {  // 其他未实现的语句类型
      rc = RC::UNIMPLEMENTED;  // 设置返回码为未实现
    }
  }
  return rc;  // 返回最终的返回码
}

// create_plan函数用于根据计算语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));  // 创建计算逻辑操作符
  return RC::SUCCESS;  // 返回成功
}

// create_plan函数用于根据选择语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> *last_oper = nullptr;  // 初始化最后一个操作符指针

  unique_ptr<LogicalOperator> table_oper(nullptr);  // 初始化表操作符
  last_oper = &table_oper;  // 设置最后一个操作符指针

  const std::vector<Table *> &tables = select_stmt->tables();  // 获取选择语句中的表列表
  for (Table *table : tables) {  // 遍历每个表
    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_ONLY));  // 创建表获取逻辑操作符
    if (table_oper == nullptr) {  // 如果当前没有表操作符，则设置当前操作符为表获取操作符
      table_oper = std::move(table_get_oper);
    } else {  // 如果已经有表操作符，则创建连接操作符并将当前表操作符和表获取操作符作为子操作符
      JoinLogicalOperator *join_oper = new JoinLogicalOperator;
      join_oper->add_child(std::move(table_oper));
      join_oper->add_child(std::move(table_get_oper));
      table_oper = unique_ptr<LogicalOperator>(join_oper);
    }
  }

  unique_ptr<LogicalOperator> predicate_oper;  // 初始化谓词操作符

  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);  // 创建谓词逻辑计划
  if (rc != RC::SUCCESS) {  // 如果创建谓词逻辑计划失败，则记录日志并返回失败
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  if (predicate_oper) {  // 如果有谓词操作符
    if (*last_oper) {  // 如果最后一个操作符存在，则将谓词操作符的子操作符设置为最后一个操作符
      predicate_oper->add_child(std::move(*last_oper));
    }

    last_oper = &predicate_oper;  // 更新最后一个操作符指针
  }

  // 注意：函数结束时没有返回值，这可能是一个错误，应该返回逻辑操作符或返回码
}

unique_ptr<LogicalOperator> group_by_oper;  // 创建一个智能指针，用于存储GROUP BY操作的逻辑操作符
rc = create_group_by_plan(select_stmt, group_by_oper);  // 创建GROUP BY逻辑计划
if (OB_FAIL(rc)) {  // 如果创建GROUP BY逻辑计划失败
  LOG_WARN("failed to create group by logical plan. rc=%s", strrc(rc));  // 记录警告日志
  return rc;  // 返回失败的返回码
}

if (group_by_oper) {  // 如果GROUP BY操作符存在
  if (*last_oper) {  // 如果最后一个操作符存在
    group_by_oper->add_child(std::move(*last_oper));  // 将最后一个操作符作为GROUP BY操作符的子操作符
  }

  last_oper = &group_by_oper;  // 更新最后一个操作符指针
}

// 创建PROJECT操作符，将查询表达式作为PROJECT操作符的子操作符
auto project_oper = make_unique<ProjectLogicalOperator>(std::move(select_stmt->query_expressions()));
if (*last_oper) {  // 如果最后一个操作符存在
  project_oper->add_child(std::move(*last_oper));  // 将最后一个操作符作为PROJECT操作符的子操作符
}

logical_operator = std::move(project_oper);  // 将PROJECT操作符赋值给输出参数
return RC::SUCCESS;  // 返回成功

// create_plan函数用于根据过滤语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator) {
  RC rc = RC::SUCCESS;  // 初始化返回码为成功
  std::vector<unique_ptr<Expression>> cmp_exprs;  // 创建一个表达式列表，用于存储比较表达式
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();  // 获取过滤语句的过滤单元列表
  for (const FilterUnit *filter_unit : filter_units) {  // 遍历每个过滤单元
    const FilterObj &filter_obj_left = filter_unit->left();  // 获取左过滤对象
    const FilterObj &filter_obj_right = filter_unit->right();  // 获取右过滤对象

    // 根据过滤对象的类型创建表达式
    unique_ptr<Expression> left(filter_obj_left.is_attr
                                ? static_cast<Expression *>(new FieldExpr(filter_obj_left.field))
                                : static_cast<Expression *>(new ValueExpr(filter_obj_left.value)));

    unique_ptr<Expression> right(filter_obj_right.is_attr
                                ? static_cast<Expression *>(new FieldExpr(filter_obj_right.field))
                                : static_cast<Expression *>(new ValueExpr(filter_obj_right.value)));

    // 检查左右表达式的值类型是否匹配，如果不匹配则进行类型转换
    if (left->value_type() != right->value_type()) {
      auto left_to_right_cost = implicit_cast_cost(left->value_type(), right->value_type());
      auto right_to_left_cost = implicit_cast_cost(right->value_type(), left->value_type());
      if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX) {
        // 如果向左向右转换成本相同或向左转换成本低，则将左表达式转换为右表达式的类型
        ExprType left_type = left->type();
        auto cast_expr = make_unique<CastExpr>(std::move(left), right->value_type());
        if (left_type == ExprType::VALUE) {
          Value left_val;
          if (OB_FAIL(rc = cast_expr->try_get_value(left_val))) {  // 尝试获取转换后的值
            LOG_WARN("failed to get value from left child", strrc(rc));
            return rc;
          }
          left = make_unique<ValueExpr>(left_val);  // 创建一个新的值表达式
        } else {
          left = std::move(cast_expr);  // 更新左表达式
        }
      } else if (right_to_left_cost < left_to_right_cost && right_to_left_cost != INT32_MAX) {
        // 如果向右转换成本低，则将右表达式转换为左表达式的类型
        ExprType right_type = right->type();
        auto cast_expr = make_unique<CastExpr>(std::move(right), left->value_type());
        if (right_type == ExprType::VALUE) {
          Value right_val;
          if (OB_FAIL(rc = cast_expr->try_get_value(right_val))) {  // 尝试获取转换后的值
            LOG_WARN("failed to get value from right child", strrc(rc));
            return rc;
          }
          right = make_unique<ValueExpr>(right_val);  // 创建一个新的值表达式
        } else {
          right = std::move(cast_expr);  // 更新右表达式
        }
      } else {
        rc = RC::UNSUPPORTED;  // 如果类型转换不支持
        LOG_WARN("unsupported cast from %s to %s", attr_type_to_string(left->value_type()), attr_type_to_string(right->value_type()));
        return rc;
      }
    }

    // 创建比较表达式并添加到列表中
    ComparisonExpr *cmp_expr = new ComparisonExpr(filter_unit->comp(), std::move(left), std::move(right));
    cmp_exprs.emplace_back(cmp_expr);
  }

  unique_ptr<PredicateLogicalOperator> predicate_oper;  // 创建一个谓词逻辑操作符的智能指针
  if (!cmp_exprs.empty()) {  // 如果比较表达式列表不为空
    // 创建一个合取表达式，并将比较表达式作为子表达式
    unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(conjunction_expr)));
  }

  logical_operator = std::move(predicate_oper);  // 将谓词逻辑操作符赋值给输出参数
  return rc;  // 返回返回码
}
// implicit_cast_cost函数用于计算从一种数据类型隐式转换到另一种数据类型的成本
int LogicalPlanGenerator::implicit_cast_cost(AttrType from, AttrType to) {
  if (from == to) {  // 如果数据类型相同，则成本为0
    return 0;
  }
  // 否则，返回从源数据类型到目标数据类型的转换成本
  return DataType::type_instance(from)->cast_cost(to);
}

// create_plan函数用于根据插入语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator) {
  Table *table = insert_stmt->table();  // 获取插入语句的目标表
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());  // 获取插入语句的值列表

  // 创建插入逻辑操作符，并设置目标表和值列表
  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);  // 将插入逻辑操作符赋值给输出参数
  return RC::SUCCESS;  // 返回成功
}

// create_plan函数用于根据删除语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator) {
  Table *table = delete_stmt->table();  // 获取删除语句的目标表
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();  // 获取删除语句的过滤条件

  // 创建表获取逻辑操作符，设置目标表和读写模式
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));

  unique_ptr<LogicalOperator> predicate_oper;  // 创建一个谓词逻辑操作符的智能指针

  RC rc = create_plan(filter_stmt, predicate_oper);  // 创建过滤条件的逻辑计划
  if (rc != RC::SUCCESS) {  // 如果创建过滤条件的逻辑计划失败
    return rc;  // 返回失败的返回码
  }

  // 创建删除逻辑操作符，并设置目标表
  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {  // 如果有过滤条件
    predicate_oper->add_child(std::move(table_get_oper));  // 将表获取逻辑操作符作为过滤条件的子操作符
    delete_oper->add_child(std::move(predicate_oper));  // 将过滤条件作为删除逻辑操作符的子操作符
  } else {  // 如果没有过滤条件
    delete_oper->add_child(std::move(table_get_oper));  // 将表获取逻辑操作符作为删除逻辑操作符的子操作符
  }

  logical_operator = std::move(delete_oper);  // 将删除逻辑操作符赋值给输出参数
  return rc;  // 返回返回码
}

// create_plan函数用于根据解释语句生成逻辑操作符
RC LogicalPlanGenerator::create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator) {
  unique_ptr<LogicalOperator> child_oper;  // 创建一个子逻辑操作符的智能指针

  Stmt *child_stmt = explain_stmt->child();  // 获取解释语句的子语句

  RC rc = create(child_stmt, child_oper);  // 创建子语句的逻辑计划
  if (rc != RC::SUCCESS) {  // 如果创建子语句的逻辑计划失败
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);  // 创建解释逻辑操作符
  logical_operator->add_child(std::move(child_oper));  // 将子逻辑操作符作为解释逻辑操作符的子操作符
  return rc;  // 返回返回码
}

// create_group_by_plan函数用于根据选择语句生成GROUP BY逻辑操作符
RC LogicalPlanGenerator::create_group_by_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator) {
  // 获取GROUP BY表达式列表
  vector<unique_ptr<Expression>> &group_by_expressions = select_stmt->group_by();
  vector<Expression *> aggregate_expressions;  // 用于存储聚合表达式的指针
  // 获取查询表达式列表
  vector<unique_ptr<Expression>> &query_expressions = select_stmt->query_expressions();

  // 定义一个函数，用于收集聚合表达式
  function<RC(unique_ptr<Expression>&)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {  // 如果是聚合表达式
      expr->set_pos(aggregate_expressions.size() + group_by_expressions.size());  // 设置位置
      aggregate_expressions.push_back(expr.get());  // 添加到聚合表达式列表
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, collector);  // 递归处理子表达式
    return rc;
  };

  // 定义一个函数，用于绑定GROUP BY表达式
  function<RC(unique_ptr<Expression>&)> bind_group_by_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    for (size_t i = 0; i < group_by_expressions.size(); i++) {  // 遍历GROUP BY表达式
      auto &group_by = group_by_expressions[i];
      if (expr->type() == ExprType::AGGREGATION) {  // 如果是聚合表达式，不需要绑定
        break;
      } else if (expr->equal(*group_by)) {  // 如果表达式与GROUP BY表达式相等
        expr->set_pos(i);  // 设置位置
        continue;  // 继续下一个表达式
      } else {
        rc = ExpressionIterator::iterate_child_expr(*expr, bind_group_by_expr);  // 递归处理子表达式
      }
    }
    return rc;
  };

  // 定义一个变量，用于标记是否找到未绑定的列
  bool found_unbound_column = false;
  // 定义一个函数，用于查找未绑定的列
  function<RC(unique_ptr<Expression>&)> find_unbound_column = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {  // 如果是聚合表达式，不需要处理
      // do nothing
    } else if (expr->pos() != -1) {  // 如果已经绑定了位置，不需要处理
      // do nothing
    } else if (expr->type() == ExprType::FIELD) {  // 如果是字段表达式，标记找到未绑定的列
      found_unbound_column = true;
    } else {
      rc = ExpressionIterator::iterate_child_expr(*expr, find_unbound_column);  // 递归处理子表达式
    }
    return rc;
  };

  // 绑定查询表达式中的GROUP BY表达式
  for (unique_ptr<Expression> &expression : query_expressions) {
    bind_group_by_expr(expression);
  }

  // 查找查询表达式中的未绑定列
  for (unique_ptr<Expression> &expression : query_expressions) {
    find_unbound_column(expression);
  }

  // 收集所有聚合表达式
  for (unique_ptr<Expression> &expression : query_expressions) {
    collector(expression);
  }

  // 如果没有GROUP BY表达式和聚合表达式，不需要生成GROUP BY逻辑操作符
  if (group_by_expressions.empty() && aggregate_expressions.empty()) {
    return RC::SUCCESS;
  }

  // 如果找到未绑定的列，返回错误
  if (found_unbound_column) {
    LOG_WARN("column must appear in the GROUP BY clause or must be part of an aggregate function");
    return RC::INVALID_ARGUMENT;
  }

  // 如果只需要聚合，但是没有GROUP BY语句，需要生成一个空的GROUP BY语句
  auto group_by_oper = make_unique<GroupByLogicalOperator>(std::move(group_by_expressions), std::move(aggregate_expressions));
  logical_operator = std::move(group_by_oper);  // 将GROUP BY逻辑操作符赋值给输出参数
  return RC::SUCCESS;  // 返回成功
}