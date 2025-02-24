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

// 引入必要的头文件
#pragma once  // 确保头文件内容只被包含一次
#include <string>  // 包含标准字符串库
#include <vector>  // 包含标准向量库

#include "sql/stmt/stmt.h"  // 引入基础的SQL语句类定义

/**
 * @brief 代表事务的Begin语句
 * @ingroup Statement
 */
class TrxBeginStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数
  TrxBeginStmt() {}
  
  // 虚析构函数，确保派生类的析构函数被正确调用
  virtual ~TrxBeginStmt() = default;

  // 返回语句类型，这里是BEGIN
  StmtType type() const override { return StmtType::BEGIN; }

  // 静态方法，用于创建TrxBeginStmt对象
  static RC create(Stmt *&stmt)
  {
    stmt = new TrxBeginStmt();  // 在堆上动态创建TrxBeginStmt对象
    return RC::SUCCESS;  // 返回成功状态码
  }
};