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
// Created by Wangyunlai on 2022/07/08.
// 文件创建时间：2022年7月8日
//

#pragma once  // 防止头文件被重复包含

#include "sql/expr/tuple.h"                  // 引入 Tuple 类，用于表示元组
#include "sql/operator/physical_operator.h"  // 引入物理操作符基类
#include "storage/record/record_manager.h"   // 引入记录管理器

/**
 * @brief 索引扫描物理算子
 * @ingroup PhysicalOperator
 * 该类实现了通过索引进行的扫描操作
 */
class IndexScanPhysicalOperator : public PhysicalOperator  // 继承自物理操作符基类
{
public:
  /**
   * @brief 构造函数
   * @param table 操作的表
   * @param index 使用的索引
   * @param mode 读取或写入模式
   * @param left_value 范围左侧的值
   * @param left_inclusive 左侧边界是否包含
   * @param right_value 范围右侧的值
   * @param right_inclusive 右侧边界是否包含
   */
  IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode, const Value *left_value,
      bool left_inclusive, const Value *right_value, bool right_inclusive);

  virtual ~IndexScanPhysicalOperator() = default;  // 默认析构函数

  PhysicalOperatorType type() const override { return PhysicalOperatorType::INDEX_SCAN; }  // 返回物理操作符类型

  std::string param() const override;  // 获取参数的字符串表示

  RC open(Trx *trx) override;  // 打开操作符，准备进行扫描
  RC next() override;          // 获取下一个结果元组
  RC close() override;         // 关闭操作符，释放资源

  Tuple *current_tuple() override;  // 获取当前元组

  /**
   * @brief 设置过滤条件
   * @param exprs 过滤条件表达式的唯一指针
   */
  void set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs);

private:
  // 与 TableScanPhysicalOperator 代码相同，可以优化
  /**
   * @brief 过滤函数
   * @param tuple 要过滤的行元组
   * @param result 过滤结果
   * @return 处理结果代码
   */
  RC filter(RowTuple &tuple, bool &result);

private:
  Trx               *trx_            = nullptr;                    // 当前事务指针
  Table             *table_          = nullptr;                    // 操作的表指针
  Index             *index_          = nullptr;                    // 使用的索引指针
  ReadWriteMode      mode_           = ReadWriteMode::READ_WRITE;  // 读取/写入模式
  IndexScanner      *index_scanner_  = nullptr;                    // 索引扫描器指针
  RecordFileHandler *record_handler_ = nullptr;                    // 记录文件处理器指针

  Record   current_record_;  // 当前记录
  RowTuple tuple_;           // 当前行元组

  Value left_value_;               // 范围左侧值
  Value right_value_;              // 范围右侧值
  bool  left_inclusive_  = false;  // 左侧是否包含边界
  bool  right_inclusive_ = false;  // 右侧是否包含边界

  std::vector<std::unique_ptr<Expression>> predicates_;  // 过滤条件的表达式列表
};
