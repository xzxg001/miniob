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

#include "sql/operator/index_scan_physical_operator.h"  // 包含索引扫描物理算子的头文件
#include "storage/index/index.h"                        // 引入索引类
#include "storage/trx/trx.h"                            // 引入事务类

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
IndexScanPhysicalOperator::IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode,
    const Value *left_value, bool left_inclusive, const Value *right_value, bool right_inclusive)
    : table_(table),                     // 初始化表指针
      index_(index),                     // 初始化索引指针
      mode_(mode),                       // 初始化读取/写入模式
      left_inclusive_(left_inclusive),   // 初始化左侧是否包含边界
      right_inclusive_(right_inclusive)  // 初始化右侧是否包含边界
{
  if (left_value) {             // 如果左侧值不为空
    left_value_ = *left_value;  // 赋值给左侧值
  }
  if (right_value) {              // 如果右侧值不为空
    right_value_ = *right_value;  // 赋值给右侧值
  }
}

/**
 * @brief 打开索引扫描操作符，准备进行扫描
 * @param trx 当前事务
 * @return 处理结果代码
 */
RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if (nullptr == table_ || nullptr == index_) {  // 检查表和索引是否有效
    return RC::INTERNAL;                         // 返回内部错误
  }

  // 创建索引扫描器
  IndexScanner *index_scanner = index_->create_scanner(left_value_.data(),
      left_value_.length(),                      // 左侧值数据和长度
      left_inclusive_,                           // 左侧是否包含
      right_value_.data(),                       // 右侧值数据
      right_value_.length(),                     // 右侧值长度
      right_inclusive_);                         // 右侧是否包含
  if (nullptr == index_scanner) {                // 检查索引扫描器是否创建成功
    LOG_WARN("failed to create index scanner");  // 记录警告日志
    return RC::INTERNAL;                         // 返回内部错误
  }

  // 获取记录处理器
  record_handler_ = table_->record_handler();
  if (nullptr == record_handler_) {      // 检查记录处理器是否有效
    LOG_WARN("invalid record handler");  // 记录警告日志
    index_scanner->destroy();            // 销毁索引扫描器
    return RC::INTERNAL;                 // 返回内部错误
  }
  index_scanner_ = index_scanner;  // 保存索引扫描器

  tuple_.set_schema(table_, table_->table_meta().field_metas());  // 设置元组的模式

  trx_ = trx;          // 保存当前事务
  return RC::SUCCESS;  // 返回成功
}

/**
 * @brief 获取下一个结果元组
 * @return 处理结果代码
 */
RC IndexScanPhysicalOperator::next()
{
  RID rid;               // 定义记录标识符
  RC  rc = RC::SUCCESS;  // 初始化返回代码为成功

  bool filter_result = false;                                       // 过滤结果初始化为 false
  while (RC::SUCCESS == (rc = index_scanner_->next_entry(&rid))) {  // 循环获取下一个索引条目
    rc = record_handler_->get_record(rid, current_record_);         // 根据 RID 获取记录
    if (OB_FAIL(rc)) {                                              // 检查获取记录的结果
      LOG_TRACE("failed to get record. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));  // 记录失败日志
      return rc;                                                                           // 返回错误代码
    }

    LOG_TRACE("got a record. rid=%s", rid.to_string().c_str());  // 记录成功日志

    tuple_.set_record(&current_record_);                      // 设置当前元组为获取到的记录
    rc = filter(tuple_, filter_result);                       // 对元组进行过滤
    if (OB_FAIL(rc)) {                                        // 检查过滤的结果
      LOG_TRACE("failed to filter record. rc=%s", strrc(rc));  // 记录过滤失败日志
      return rc;                                              // 返回错误代码
    }

    if (!filter_result) {            // 如果过滤结果为 false
      LOG_TRACE("record filtered");  // 记录被过滤
      continue;                      // 继续下一个循环
    }

    // 访问记录
    rc = trx_->visit_record(table_, current_record_, mode_);
    if (rc == RC::RECORD_INVISIBLE) {  // 检查记录是否可见
      LOG_TRACE("record invisible");   // 记录不可见日志
      continue;                        // 继续下一个循环
    } else {
      return rc;  // 返回其他结果代码
    }
  }

  return rc;  // 返回最终的结果代码
}

/**
 * @brief 关闭索引扫描操作符，释放资源
 * @return 处理结果代码
 */
RC IndexScanPhysicalOperator::close()
{
  index_scanner_->destroy();  // 销毁索引扫描器
  index_scanner_ = nullptr;   // 清空索引扫描器指针
  return RC::SUCCESS;         // 返回成功
}

/**
 * @brief 获取当前元组
 * @return 当前元组指针
 */
Tuple *IndexScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);  // 设置当前元组的记录
  return &tuple_;                       // 返回当前元组
}

/**
 * @brief 设置过滤条件
 * @param exprs 过滤条件表达式的唯一指针
 */
void IndexScanPhysicalOperator::set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);  // 移动过滤条件表达式
}

/**
 * @brief 过滤函数
 * @param tuple 要过滤的行元组
 * @param result 过滤结果
 * @return 处理结果代码
 */
RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;  // 初始化返回代码为成功
  Value value;             // 定义值

  // 遍历所有过滤条件
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);  // 获取当前元组的值
    if (rc != RC::SUCCESS) {             // 检查获取值的结果
      return rc;                         // 返回错误代码
    }

    bool tmp_result = value.get_boolean();  // 获取布尔值
    if (!tmp_result) {                      // 如果结果为 false
      result = false;                       // 设置过滤结果为 false
      return rc;                            // 返回成功
    }
  }

  result = true;  // 所有条件通过，设置过滤结果为 true
  return rc;      // 返回成功
}

/**
 * @brief 获取参数的字符串表示
 * @return 参数字符串
 */
std::string IndexScanPhysicalOperator::param() const
{
  return std::string(index_->index_meta().name()) + " ON " + table_->name();  // 返回索引名称和表名
}
