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
// Created by Wangyunlai on 2022/6/5.
//

// 引入必要的头文件
#pragma once  // 确保头文件只被包含一次
#include <vector>  // 引入标准模板库中的向量容器

// 引入项目中的其他头文件
#include "common/rc.h"  // 可能包含错误码定义
#include "sql/expr/expression.h"  // 表达式类的定义
#include "sql/stmt/stmt.h"  // 语句基类的定义

// 声明外部依赖的类
class Db;
class Table;

/**
 * @brief 描述算术运算语句
 * @ingroup Statement
 */
class CalcStmt : public Stmt  // CalcStmt类继承自Stmt类
{
public:
  // 默认构造函数
  CalcStmt() = default;
  // 虚析构函数，确保派生类的析构函数被正确调用
  virtual ~CalcStmt() override = default;

  // 返回语句类型，这里是算术运算类型
  StmtType type() const override { return StmtType::CALC; }

public:
  // 静态工厂方法，用于创建CalcStmt对象
  static RC create(CalcSqlNode &calc_sql, Stmt *&stmt)
  {
    // 创建CalcStmt对象
    CalcStmt *calc_stmt = new CalcStmt();
    // 将传入的表达式列表移动到calc_stmt对象中
    calc_stmt->expressions_ = std::move(calc_sql.expressions);
    // 将calc_stmt对象的地址赋值给stmt参数
    stmt = calc_stmt;
    // 返回成功状态码
    return RC::SUCCESS;
  }

public:
  // 提供对表达式列表的访问
  std::vector<std::unique_ptr<Expression>> &expressions() { return expressions_; }

private:
  // 存储表达式的私有成员变量，使用unique_ptr来管理表达式对象的生命周期
  std::vector<std::unique_ptr<Expression>> expressions_;
};
