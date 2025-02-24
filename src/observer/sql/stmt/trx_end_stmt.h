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

#include <string>  // 包含标准字符串库
#include <vector>  // 包含标准向量库

#include "sql/stmt/stmt.h"  // 引入基础的SQL语句类定义

/**
 * @brief 代表事务的Commit或Rollback语句
 * @ingroup Statement
 */
class TrxEndStmt : public Stmt  // 继承自Stmt基类
{
public:
  // 构造函数，接收一个StmtType类型的参数，表示事务是提交还是回滚
  TrxEndStmt(StmtType type) : type_(type) {}
  
  // 虚析构函数，使用编译器默认的析构函数实现
  virtual ~TrxEndStmt() = default;

  // 返回语句类型，这里是COMMIT或ROLLBACK
  StmtType type() const override { return type_; }

  // 静态方法，用于根据命令标志创建TrxEndStmt对象
  static RC create(SqlCommandFlag flag, Stmt *&stmt)
  {
    // 根据flag确定是提交还是回滚
    StmtType type = (flag == SqlCommandFlag::SCF_COMMIT) ? StmtType::COMMIT : StmtType::ROLLBACK;
    // 在堆上动态创建TrxEndStmt对象
    stmt = new TrxEndStmt(type);
    // 返回成功状态码
    return RC::SUCCESS;
  }

private:
  // 成员变量，用于存储事务结束的类型（提交或回滚）
  StmtType type_;
};