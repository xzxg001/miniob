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

#pragma once  // 预处理指令，确保头文件只被包含一次

#include <string>  // 引入字符串库
#include <vector>  // 引入向量容器库

#include "sql/stmt/stmt.h"  // 引入基类Stmt的声明

class Db;  // 数据库类的前向声明

/**
 * @brief 描述表的语句
 * @ingroup Statement
 * @details 虽然解析成了stmt，但是与原始的SQL解析后的数据也差不多
 */
class ShowTablesStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 默认构造函数
  ShowTablesStmt()          = default;
  
  // 默认虚析构函数
  virtual ~ShowTablesStmt() = default;

  // 返回语句类型，这里是显示表
  StmtType type() const override { return StmtType::SHOW_TABLES; }

  // 静态方法，用于创建ShowTablesStmt对象
  static RC create(Db *db, Stmt *&stmt)
  {
    stmt = new ShowTablesStmt();  // 创建ShowTablesStmt对象
    return RC::SUCCESS;  // 返回成功
  }
};