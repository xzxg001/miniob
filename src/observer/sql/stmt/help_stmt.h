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
#pragma once

#include <string>  // 引入标准库中的字符串类
#include <vector>  // 引入标准库中的向量容器

#include "sql/stmt/stmt.h"  // 引入SQL语句基类的头文件

/**
 * @brief 帮助语句类
 * @ingroup Statement
 * @details 这个类表示一个HELP语句，通常用于获取数据库操作的帮助信息。
 */
class HelpStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 默认构造函数
  HelpStmt() {}
  // 默认的虚析构函数
  virtual ~HelpStmt() = default;

  // 返回语句类型，这里是帮助语句
  StmtType type() const override { return StmtType::HELP; }

  // 静态方法，用于创建HelpStmt对象
  static RC create(Stmt *&stmt)
  {
    stmt = new HelpStmt();  // 创建HelpStmt对象并赋值给stmt参数
    return RC::SUCCESS;  // 返回成功状态码
  }
};