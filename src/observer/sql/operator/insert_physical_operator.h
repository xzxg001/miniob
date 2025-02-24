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
// Created by WangYunlai on 2021/6/7.
// 文件创建时间：2021年6月7日
//

#pragma once

#include "sql/operator/physical_operator.h"  // 包含物理算子的基类
#include "sql/parser/parse.h"                // 包含解析相关的定义
#include <vector>                            // 使用向量容器
class InsertStmt;                            // 前向声明，避免循环依赖

/**
 * @brief 插入物理算子
 * @ingroup PhysicalOperator
 * 该类负责处理数据库中的插入操作
 */
class InsertPhysicalOperator : public PhysicalOperator
{
public:
  // 构造函数
  InsertPhysicalOperator(Table *table, std::vector<Value> &&values);

  virtual ~InsertPhysicalOperator() = default;  // 默认析构函数

  // 重写物理算子类型函数，返回插入类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::INSERT; }

  // 打开物理算子，准备执行插入操作
  RC open(Trx *trx) override;

  // 获取下一个插入操作的状态
  RC next() override;

  // 关闭物理算子，清理资源
  RC close() override;

  // 返回当前元组，插入操作不需要返回元组
  Tuple *current_tuple() override { return nullptr; }

private:
  Table             *table_ = nullptr;  // 指向目标表的指针
  std::vector<Value> values_;           // 存储要插入的值
};
