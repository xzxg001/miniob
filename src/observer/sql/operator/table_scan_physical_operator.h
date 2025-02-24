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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include "common/rc.h"                       // 引入返回代码定义
#include "sql/operator/physical_operator.h"  // 引入物理算子的基类
#include "storage/record/record_manager.h"   // 引入记录管理器
#include "common/types.h"                    // 引入常用类型定义

class Table;  // 前向声明 Table 类

/**
 * @brief 表扫描物理算子
 * @ingroup PhysicalOperator
 */
class TableScanPhysicalOperator : public PhysicalOperator
{
public:
  // 构造函数，接受表和读写模式
  TableScanPhysicalOperator(Table *table, ReadWriteMode mode) : table_(table), mode_(mode) {}

  virtual ~TableScanPhysicalOperator() = default;  // 默认析构函数

  // 获取参数信息的字符串
  std::string param() const override;

  // 返回物理算子的类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::TABLE_SCAN; }

  // 打开算子
  RC open(Trx *trx) override;
  // 获取下一个记录
  RC next() override;
  // 关闭算子
  RC close() override;

  // 获取当前元组
  Tuple *current_tuple() override;

  // 设置过滤条件
  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);

private:
  // 根据过滤条件对元组进行过滤
  RC filter(RowTuple &tuple, bool &result);

private:
  Table                                   *table_ = nullptr;                    // 目标表
  Trx                                     *trx_   = nullptr;                    // 当前事务
  ReadWriteMode                            mode_  = ReadWriteMode::READ_WRITE;  // 读写模式
  RecordFileScanner                        record_scanner_;                     // 记录扫描器
  Record                                   current_record_;                     // 当前记录
  RowTuple                                 tuple_;                              // 当前元组
  std::vector<std::unique_ptr<Expression>> predicates_;                         // 过滤条件表达式
};
