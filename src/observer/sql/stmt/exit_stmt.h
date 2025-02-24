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
// Created by Wangyunlai on 2023/6/14.
//
// 防止头文件被重复包含
#pragma once

// 引入SQL语句基类的头文件
#include "sql/stmt/stmt.h"

/**
 * @brief 退出语句类
 * @ingroup Statement
 * @details 表示断开连接的语句，目前没有成员变量
 */
class ExitStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 默认构造函数
  ExitStmt() {}
  // 默认的虚析构函数
  virtual ~ExitStmt() = default;

  // 返回语句类型，这里是退出语句
  StmtType type() const override { return StmtType::EXIT; }

  // 静态方法，用于创建ExitStmt对象
  static RC create(Stmt *&stmt)
  {
    stmt = new ExitStmt();  // 创建ExitStmt对象并赋值给stmt参数
    return RC::SUCCESS;  // 返回成功状态码
  }
};