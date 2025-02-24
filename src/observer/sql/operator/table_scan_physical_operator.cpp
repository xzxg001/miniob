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
//

#include "sql/operator/table_scan_physical_operator.h"  // 引入表扫描物理算子的头文件
#include "event/sql_debug.h"                            // 引入 SQL 调试功能
#include "storage/table/table.h"                        // 引入表类

using namespace std;  // 使用标准命名空间

// 打开表扫描物理算子
RC TableScanPhysicalOperator::open(Trx *trx)
{
  // 获取记录扫描器
  RC rc = table_->get_record_scanner(record_scanner_, trx, mode_);
  if (rc == RC::SUCCESS) {
    // 设置元组的模式
    tuple_.set_schema(table_, table_->table_meta().field_metas());
  }
  trx_ = trx;  // 保存当前事务
  return rc;   // 返回结果代码
}

// 获取下一个符合条件的记录
RC TableScanPhysicalOperator::next()
{
  RC   rc            = RC::SUCCESS;  // 初始化结果代码
  bool filter_result = false;        // 过滤结果

  // 迭代获取记录
  while (OB_SUCC(rc = record_scanner_.next(current_record_))) {
    LOG_TRACE("got a record. rid=%s", current_record_.rid().to_string().c_str());

    // 设置当前记录到元组
    tuple_.set_record(&current_record_);
    rc = filter(tuple_, filter_result);  // 过滤元组

    if (rc != RC::SUCCESS) {
      LOG_TRACE("record filtered failed=%s", strrc(rc));
      return rc;  // 过滤失败，返回错误代码
    }

    if (filter_result) {                                         // 如果过滤通过
      sql_debug("get a tuple: %s", tuple_.to_string().c_str());  // 调试日志
      break;                                                     // 结束循环
    } else {
      sql_debug("a tuple is filtered: %s", tuple_.to_string().c_str());  // 被过滤的元组
    }
  }
  return rc;  // 返回结果代码
}

// 关闭表扫描物理算子
RC TableScanPhysicalOperator::close() { return record_scanner_.close_scan(); }

// 返回当前元组
Tuple *TableScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);  // 设置当前记录
  return &tuple_;                       // 返回当前元组
}

// 返回参数字符串（表名）
string TableScanPhysicalOperator::param() const { return table_->name(); }

// 设置过滤条件
void TableScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);  // 移动到成员变量
}

// 过滤元组，检查是否符合所有条件
RC TableScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;  // 初始化结果代码
  Value value;             // 存储表达式值

  // 遍历每个过滤条件
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);  // 获取表达式的值
    if (rc != RC::SUCCESS) {
      return rc;  // 如果获取失败，返回错误代码
    }

    bool tmp_result = value.get_boolean();  // 获取布尔结果
    if (!tmp_result) {                      // 如果结果为假，返回过滤失败
      result = false;
      return rc;
    }
  }

  result = true;  // 所有条件通过，结果为真
  return rc;      // 返回结果代码
}
