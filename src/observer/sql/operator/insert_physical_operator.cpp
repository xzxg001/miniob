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
// Created by WangYunlai on 2021/6/9.
// 文件创建时间：2021年6月9日
//

#include "sql/operator/insert_physical_operator.h"  // 包含插入物理算子的头文件
#include "sql/stmt/insert_stmt.h"                   // 包含插入语句的头文件
#include "storage/table/table.h"                    // 包含表的头文件
#include "storage/trx/trx.h"                        // 包含事务的头文件

using namespace std;  // 使用标准命名空间

// 构造函数：初始化目标表和要插入的值
InsertPhysicalOperator::InsertPhysicalOperator(Table *table, vector<Value> &&values)
    : table_(table), values_(std::move(values))  // 使用 std::move 移动值以避免拷贝
{}

// 打开物理算子，准备插入记录
RC InsertPhysicalOperator::open(Trx *trx)
{
  Record record;  // 定义记录对象
  // 创建记录，参数包括字段数量、字段值数组和记录对象
  RC rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to make record. rc=%s", strrc(rc));  // 记录错误信息
    return rc;                                           // 返回错误代码
  }

  // 通过事务插入记录
  rc = trx->insert_record(table_, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));  // 记录错误信息
  }
  return rc;  // 返回插入结果的错误代码
}

// 获取下一个插入操作的状态
RC InsertPhysicalOperator::next() { return RC::RECORD_EOF; }  // 表示插入操作结束

// 关闭物理算子，清理资源
RC InsertPhysicalOperator::close() { return RC::SUCCESS; }  // 返回成功状态
