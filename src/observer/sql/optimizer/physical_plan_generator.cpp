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
// Created by Wangyunlai on 2022/12/14.
//

#include <utility>

#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/aggregate_vec_physical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/calc_physical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/delete_physical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/explain_physical_operator.h"
#include "sql/operator/expr_vec_physical_operator.h"
#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/operator/index_scan_physical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/insert_physical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/join_physical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/project_vec_physical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/group_by_physical_operator.h"
#include "sql/operator/hash_group_by_physical_operator.h"
#include "sql/operator/scalar_group_by_physical_operator.h"
#include "sql/operator/table_scan_vec_physical_operator.h"
#include "sql/optimizer/physical_plan_generator.h"

using namespace std;

// create函数用于根据逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 根据逻辑操作符的类型进行不同的处理
  switch (logical_operator.type()) {
    case LogicalOperatorType::CALC: {  // 计算逻辑操作符
      return create_plan(static_cast<CalcLogicalOperator &>(logical_operator), oper);  // 创建计算物理操作符
    } break;

    case LogicalOperatorType::TABLE_GET: {  // 表获取逻辑操作符
      return create_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);  // 创建表获取物理操作符
    } break;

    case LogicalOperatorType::PREDICATE: {  // 谓词逻辑操作符
      return create_plan(static_cast<PredicateLogicalOperator &>(logical_operator), oper);  // 创建谓词物理操作符
    } break;

    case LogicalOperatorType::PROJECTION: {  // 投影逻辑操作符
      return create_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);  // 创建投影物理操作符
    } break;

    case LogicalOperatorType::INSERT: {  // 插入逻辑操作符
      return create_plan(static_cast<InsertLogicalOperator &>(logical_operator), oper);  // 创建插入物理操作符
    } break;

    case LogicalOperatorType::DELETE: {  // 删除逻辑操作符
      return create_plan(static_cast<DeleteLogicalOperator &>(logical_operator), oper);  // 创建删除物理操作符
    } break;

    case LogicalOperatorType::EXPLAIN: {  // 解释逻辑操作符
      return create_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);  // 创建解释物理操作符
    } break;

    case LogicalOperatorType::JOIN: {  // 连接逻辑操作符
      return create_plan(static_cast<JoinLogicalOperator &>(logical_operator), oper);  // 创建连接物理操作符
    } break;

    case LogicalOperatorType::GROUP_BY: {  // GROUP BY逻辑操作符
      return create_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);  // 创建GROUP BY物理操作符
    } break;

    default: {  // 未知的逻辑操作符类型
      ASSERT(false, "unknown logical operator type");  // 断言未知类型
      return RC::INVALID_ARGUMENT;  // 返回无效参数的返回码
    }
  }
  return rc;  // 返回返回码
}

// create_vec函数用于根据逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 根据逻辑操作符的类型进行不同的处理
  switch (logical_operator.type()) {
    case LogicalOperatorType::TABLE_GET: {  // 表获取逻辑操作符
      return create_vec_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);  // 创建向量化表获取物理操作符
    } break;
    case LogicalOperatorType::PROJECTION: {  // 投影逻辑操作符
      return create_vec_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);  // 创建向量化投影物理操作符
    } break;
    case LogicalOperatorType::GROUP_BY: {  // GROUP BY逻辑操作符
      return create_vec_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);  // 创建向量化GROUP BY物理操作符
    } break;
    case LogicalOperatorType::EXPLAIN: {  // 解释逻辑操作符
      return create_vec_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);  // 创建向量化解释物理操作符
    } break;
    default: {  // 未知的逻辑操作符类型
      return RC::INVALID_ARGUMENT;  // 返回无效参数的返回码
    }
  }
  return rc;  // 返回返回码
}

// create_vec函数用于根据逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  // 根据逻辑操作符的类型进行不同的处理
  switch (logical_operator.type()) {
    case LogicalOperatorType::TABLE_GET: {  // 表获取逻辑操作符
      return create_vec_plan(static_cast<TableGetLogicalOperator &>(logical_operator), oper);  // 创建向量化表获取物理操作符
    } break;
    case LogicalOperatorType::PROJECTION: {  // 投影逻辑操作符
      return create_vec_plan(static_cast<ProjectLogicalOperator &>(logical_operator), oper);  // 创建向量化投影物理操作符
    } break;
    case LogicalOperatorType::GROUP_BY: {  // GROUP BY逻辑操作符
      return create_vec_plan(static_cast<GroupByLogicalOperator &>(logical_operator), oper);  // 创建向量化GROUP BY物理操作符
    } break;
    case LogicalOperatorType::EXPLAIN: {  // 解释逻辑操作符
      return create_vec_plan(static_cast<ExplainLogicalOperator &>(logical_operator), oper);  // 创建向量化解释物理操作符
    } break;
    default: {  // 未知的逻辑操作符类型
      return RC::INVALID_ARGUMENT;  // 返回无效参数的返回码
    }
  }
  return rc;  // 返回返回码
}

// create_plan函数用于根据表获取逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(TableGetLogicalOperator &table_get_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();  // 获取谓词表达式
  Table *table = table_get_oper.table();  // 获取表对象

  // 遍历谓词表达式，寻找可用于索引查找的表达式
  for (auto &expr : predicates) {
    if (expr->type() == ExprType::COMPARISON) {  // 比较表达式
      auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());  // 转换为比较表达式对象
      // 简单处理，只寻找等值查询
      if (comparison_expr->comp() != EQUAL_TO) {
        continue;
      }

      unique_ptr<Expression> &left_expr  = comparison_expr->left();
      unique_ptr<Expression> &right_expr = comparison_expr->right();
      // 左右比较的一边至少是一个值
      if (left_expr->type() != ExprType::VALUE && right_expr->type() != ExprType::VALUE) {
        continue;
      }

      FieldExpr *field_expr = nullptr;
      ValueExpr *value_expr = nullptr;
      // 确定字段表达式和值表达式
      if (left_expr->type() == ExprType::FIELD) {
        field_expr = static_cast<FieldExpr *>(left_expr.get());
        value_expr = static_cast<ValueExpr *>(right_expr.get());
      } else if (right_expr->type() == ExprType::FIELD) {
        field_expr = static_cast<FieldExpr *>(right_expr.get());
        value_expr = static_cast<ValueExpr *>(left_expr.get());
      }

      // 如果找到了字段表达式，则尝试寻找对应的索引
      if (field_expr != nullptr) {
        const Field &field = field_expr->field();
        Index *index = table->find_index_by_field(field.field_name());
        if (index != nullptr) {  // 如果找到了索引
          break;  // 退出循环
        }
      }
    }
  }

  // 如果找到了索引，则创建索引扫描物理操作符
  if (index != nullptr && value_expr != nullptr) {
    const Value &value = value_expr->get_value();
    IndexScanPhysicalOperator *index_scan_oper = new IndexScanPhysicalOperator(table,
        index,
        table_get_oper.read_write_mode(),
        &value,
        true /*left_inclusive*/,
        &value,
        true /*right_inclusive*/);

    index_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(index_scan_oper);
    LOG_TRACE("use index scan");
  } else {
    // 否则，创建表扫描物理操作符
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.read_write_mode());
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  }

  return RC::SUCCESS;  // 返回成功
}

// create_plan函数用于根据谓词逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(PredicateLogicalOperator &pred_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &children_opers = pred_oper.children();  // 获取子逻辑操作符
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalOperator &child_oper = *children_opers.front();  // 获取子逻辑操作符

  unique_ptr<PhysicalOperator> child_phy_oper;  // 创建子物理操作符的智能指针
  RC rc = create(child_oper, child_phy_oper);  // 递归创建子物理操作符
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;  // 如果创建失败，返回失败的返回码
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();  // 获取谓词表达式
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());  // 获取谓词表达式
  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));  // 创建谓词物理操作符
  oper->add_child(std::move(child_phy_oper));  // 添加子物理操作符
  return rc;  // 返回返回码
}

// create_plan函数用于根据投影逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(ProjectLogicalOperator &project_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &child_opers = project_oper.children();  // 获取子逻辑操作符

  unique_ptr<PhysicalOperator> child_phy_oper;  // 创建子物理操作符的智能指针

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {  // 如果有子逻辑操作符
    LogicalOperator *child_oper = child_opers.front().get();  // 获取子逻辑操作符

    rc = create(*child_oper, child_phy_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;  // 如果创建失败，返回失败的返回码
    }
  }

  auto project_operator = make_unique<ProjectPhysicalOperator>(std::move(project_oper.expressions()));  // 创建投影物理操作符
  if (child_phy_oper) {  // 如果有子物理操作符
    project_operator->add_child(std::move(child_phy_oper));  // 添加子物理操作符
  }

  oper = std::move(project_operator);  // 将投影物理操作符赋值给输出参数
  LOG_TRACE("create a project physical operator");  // 记录日志
  return rc;  // 返回返回码
}

// create_plan函数用于根据插入逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(InsertLogicalOperator &insert_oper, unique_ptr<PhysicalOperator> &oper) {
  Table *table = insert_oper.table();  // 获取目标表
  vector<Value> &values = insert_oper.values();  // 获取插入的值列表

  // 创建插入物理操作符
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(values));
  oper.reset(insert_phy_oper);  // 将插入物理操作符赋值给输出参数
  return RC::SUCCESS;  // 返回成功
}

// create_plan函数用于根据删除逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(DeleteLogicalOperator &delete_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &child_opers = delete_oper.children();  // 获取子逻辑操作符列表

  unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {  // 如果有子逻辑操作符
    LogicalOperator *child_oper = child_opers.front().get();  // 获取第一个子逻辑操作符

    rc = create(*child_oper, child_physical_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {  // 如果创建失败
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));  // 创建删除物理操作符

  if (child_physical_oper) {  // 如果有子物理操作符
    oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  }
  return rc;  // 返回返回码
}

// create_plan函数用于根据解释逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(ExplainLogicalOperator &explain_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();  // 获取子逻辑操作符列表

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);  // 创建解释物理操作符
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {  // 遍历子逻辑操作符
    unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
    rc = create(*child_oper, child_physical_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {  // 如果创建失败
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  }

  oper = std::move(explain_physical_oper);  // 将解释物理操作符赋值给输出参数
  return rc;  // 返回返回码
}

// create_plan函数用于根据连接逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(JoinLogicalOperator &join_oper, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;

  vector<unique_ptr<LogicalOperator>> &child_opers = join_oper.children();  // 获取子逻辑操作符列表
  if (child_opers.size() != 2) {  // 如果子逻辑操作符的数量不是2
    LOG_WARN("join operator should have 2 children, but have %d", child_opers.size());  // 记录警告日志
    return RC::INTERNAL;  // 返回内部错误
  }

  unique_ptr<PhysicalOperator> join_physical_oper(new NestedLoopJoinPhysicalOperator);  // 创建嵌套循环连接物理操作符
  for (auto &child_oper : child_opers) {  // 遍历子逻辑操作符
    unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
    rc = create(*child_oper, child_physical_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {  // 如果创建失败
      LOG_WARN("failed to create physical child oper. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }

    join_physical_oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  }

  oper = std::move(join_physical_oper);  // 将连接物理操作符赋值给输出参数
  return rc;  // 返回返回码
}

// create_plan函数用于根据计算逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(CalcLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;

  CalcPhysicalOperator *calc_oper = new CalcPhysicalOperator(std::move(logical_oper.expressions()));  // 创建计算物理操作符
  oper.reset(calc_oper);  // 将计算物理操作符赋值给输出参数
  return rc;  // 返回成功
}

// create_plan函数用于根据GROUP BY逻辑操作符生成物理操作符
RC PhysicalPlanGenerator::create_plan(GroupByLogicalOperator &logical_oper, std::unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;

  vector<unique_ptr<Expression>> &group_by_expressions = logical_oper.group_by_expressions();  // 获取GROUP BY表达式列表
  unique_ptr<GroupByPhysicalOperator> group_by_oper;  // 创建GROUP BY物理操作符的智能指针
  if (group_by_expressions.empty()) {  // 如果没有GROUP BY表达式
    group_by_oper = make_unique<ScalarGroupByPhysicalOperator>(std::move(logical_oper.aggregate_expressions()));  // 创建标量GROUP BY物理操作符
  } else {
    group_by_oper = make_unique<HashGroupByPhysicalOperator>(std::move(logical_oper.group_by_expressions()),
        std::move(logical_oper.aggregate_expressions()));  // 创建哈希GROUP BY物理操作符
  }

  ASSERT(logical_oper.children().size() == 1, "group by operator should have 1 child");  // 断言GROUP BY操作符有一个子操作符

  LogicalOperator &child_oper = *logical_oper.children().front();  // 获取子逻辑操作符
  unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
  rc = create(child_oper, child_physical_oper);  // 递归创建子物理操作符
  if (OB_FAIL(rc)) {  // 如果创建失败
    LOG_WARN("failed to create child physical operator of group by operator. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  group_by_oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  oper = std::move(group_by_oper);  // 将GROUP BY物理操作符赋值给输出参数
  return rc;  // 返回返回码
}

// create_vec_plan函数用于根据表获取逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec_plan(TableGetLogicalOperator &table_get_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();  // 获取谓词表达式
  Table *table = table_get_oper.table();  // 获取表对象

  // 创建向量化表扫描物理操作符
  TableScanVecPhysicalOperator *table_scan_oper = new TableScanVecPhysicalOperator(table, table_get_oper.read_write_mode());
  table_scan_oper->set_predicates(std::move(predicates));  // 设置谓词表达式
  oper = unique_ptr<PhysicalOperator>(table_scan_oper);  // 将表扫描物理操作符赋值给输出参数
  LOG_TRACE("use vectorized table scan");  // 记录日志

  return RC::SUCCESS;  // 返回成功
}

// create_vec_plan函数用于根据GROUP BY逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec_plan(GroupByLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> physical_oper;  // 创建物理操作符的智能指针
  if (logical_oper.group_by_expressions().empty()) {  // 如果没有GROUP BY表达式
    physical_oper = make_unique<AggregateVecPhysicalOperator>(std::move(logical_oper.aggregate_expressions()));  // 创建向量化聚合物理操作符
  } else {
    physical_oper = make_unique<GroupByVecPhysicalOperator>(  // 创建向量化GROUP BY物理操作符
      std::move(logical_oper.group_by_expressions()), std::move(logical_oper.aggregate_expressions()));
  }

  ASSERT(logical_oper.children().size() == 1, "group by operator should have 1 child");  // 断言GROUP BY操作符有一个子操作符

  LogicalOperator &child_oper = *logical_oper.children().front();  // 获取子逻辑操作符
  unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
  rc = create_vec(child_oper, child_physical_oper);  // 递归创建子物理操作符
  if (OB_FAIL(rc)) {  // 如果创建失败
    LOG_WARN("failed to create child physical operator of group by(vec) operator. rc=%s", strrc(rc));  // 记录警告日志
    return rc;  // 返回失败的返回码
  }

  physical_oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  oper = std::move(physical_oper);  // 将物理操作符赋值给输出参数
  return rc;  // 返回返回码
}

// create_vec_plan函数用于根据投影逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec_plan(ProjectLogicalOperator &project_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &child_opers = project_oper.children();  // 获取子逻辑操作符列表

  unique_ptr<PhysicalOperator> child_phy_oper;  // 创建子物理操作符的智能指针
  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {  // 如果有子逻辑操作符
    LogicalOperator *child_oper = child_opers.front().get();  // 获取第一个子逻辑操作符
    rc = create_vec(*child_oper, child_phy_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {  // 如果创建失败
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }
  }

  auto project_operator = make_unique<ProjectVecPhysicalOperator>(std::move(project_oper.expressions()));  // 创建向量化投影物理操作符
  if (child_phy_oper != nullptr) {  // 如果有子物理操作符
    std::vector<Expression *> expressions;  // 创建表达式指针列表
    for (auto &expr : project_operator->expressions()) {  // 遍历投影表达式
      expressions.push_back(expr.get());  // 添加表达式指针
    }
    auto expr_operator = make_unique<ExprVecPhysicalOperator>(std::move(expressions));  // 创建向量化表达式物理操作符
    expr_operator->add_child(std::move(child_phy_oper));  // 添加子物理操作符
    project_operator->add_child(std::move(expr_operator));  // 添加表达式物理操作符
  }

  oper = std::move(project_operator);  // 将投影物理操作符赋值给输出参数
  LOG_TRACE("create a project physical operator");  // 记录日志
  return rc;  // 返回返回码
}

// create_vec_plan函数用于根据解释逻辑操作符生成向量化物理操作符
RC PhysicalPlanGenerator::create_vec_plan(ExplainLogicalOperator &explain_oper, unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalOperator>> &child_opers = explain_oper.children();  // 获取子逻辑操作符列表

  RC rc = RC::SUCCESS;
  // 重用解释物理操作符
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);  // 创建解释物理操作符
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {  // 遍历子逻辑操作符
    unique_ptr<PhysicalOperator> child_physical_oper;  // 创建子物理操作符的智能指针
    rc = create_vec(*child_oper, child_physical_oper);  // 递归创建子物理操作符
    if (rc != RC::SUCCESS) {  // 如果创建失败
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));  // 记录警告日志
      return rc;  // 返回失败的返回码
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));  // 添加子物理操作符
  }

  oper = std::move(explain_physical_oper);  // 将解释物理操作符赋值给输出参数
  return rc;  // 返回返回码
}