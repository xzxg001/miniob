/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/rc.h"                       // 引入返回代码定义
#include "sql/operator/physical_operator.h"  // 引入物理算子基类
#include "storage/record/record_manager.h"   // 引入记录管理器
#include "common/types.h"                    // 引入公共类型定义

class Table;  // 前向声明表类

/**
 * @brief 表扫描物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class TableScanVecPhysicalOperator : public PhysicalOperator
{
public:
  // 构造函数
  TableScanVecPhysicalOperator(Table *table, ReadWriteMode mode) : table_(table), mode_(mode) {}

  // 虚析构函数
  virtual ~TableScanVecPhysicalOperator() = default;

  // 返回参数字符串（表名）
  std::string param() const override;

  // 返回物理算子类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::TABLE_SCAN_VEC; }

  // 打开物理算子
  RC open(Trx *trx) override;

  // 获取下一个数据块
  RC next(Chunk &chunk) override;

  // 关闭物理算子
  RC close() override;

  // 设置过滤条件
  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);

private:
  // 过滤数据块
  RC filter(Chunk &chunk);

private:
  Table                                   *table_ = nullptr;                    // 指向表的指针
  ReadWriteMode                            mode_  = ReadWriteMode::READ_WRITE;  // 读写模式
  ChunkFileScanner                         chunk_scanner_;                      // 数据块扫描器
  Chunk                                    all_columns_;                        // 存储所有列的数据
  Chunk                                    filtered_columns_;                   // 存储经过过滤的列数据
  std::vector<uint8_t>                     select_;                             // 选择位图
  std::vector<std::unique_ptr<Expression>> predicates_;                         // 过滤条件
};
