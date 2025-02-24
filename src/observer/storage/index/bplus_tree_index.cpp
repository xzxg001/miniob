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
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"

// B+树索引析构函数
BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

// 创建B+树索引
RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  // 检查索引是否已经初始化
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s, field:%s",
        file_name, index_meta.name(), index_meta.field());
    return RC::RECORD_OPENNED;  // 返回记录已打开的错误
  }

  // 初始化索引元数据
  Index::init(index_meta, field_meta);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager(); // 获取缓冲池管理器
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, field_meta.type(), field_meta.len());
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name, index_meta.name(), index_meta.field(), strrc(rc));
    return rc; // 返回创建索引处理器失败的错误
  }

  inited_ = true; // 标记索引已初始化
  table_  = table; // 关联表
  LOG_INFO("Successfully create index, file_name:%s, index:%s, field:%s",
    file_name, index_meta.name(), index_meta.field());
  return RC::SUCCESS; // 返回成功
}

// 打开B+树索引
RC BplusTreeIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  // 检查索引是否已经初始化
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been initedd before. file_name:%s, index:%s, field:%s",
        file_name, index_meta.name(), index_meta.field());
    return RC::RECORD_OPENNED; // 返回记录已打开的错误
  }

  // 初始化索引元数据
  Index::init(index_meta, field_meta);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager(); // 获取缓冲池管理器
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name, index_meta.name(), index_meta.field(), strrc(rc));
    return rc; // 返回打开索引处理器失败的错误
  }

  inited_ = true; // 标记索引已初始化
  table_  = table; // 关联表
  LOG_INFO("Successfully open index, file_name:%s, index:%s, field:%s",
    file_name, index_meta.name(), index_meta.field());
  return RC::SUCCESS; // 返回成功
}

// 关闭B+树索引
RC BplusTreeIndex::close()
{
  // 如果索引已初始化，则关闭索引
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field:%s", index_meta_.name(), index_meta_.field());
    index_handler_.close();
    inited_ = false; // 标记索引未初始化
  }
  LOG_INFO("Successfully close index."); // 记录关闭成功
  return RC::SUCCESS; // 返回成功
}

// 插入记录条目
RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  return index_handler_.insert_entry(record + field_meta_.offset(), rid); // 插入操作
}

// 删除记录条目
RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  return index_handler_.delete_entry(record + field_meta_.offset(), rid); // 删除操作
}

// 创建索引扫描器
IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner; // 打开失败则删除扫描器
    return nullptr; // 返回空指针
  }
  return index_scanner; // 返回创建的扫描器
}

// 同步索引
RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
// B+树索引扫描器实现
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

// 析构函数
BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

// 打开扫描器
RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

// 获取下一个条目
RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

// 销毁扫描器
RC BplusTreeIndexScanner::destroy()
{
  delete this; // 删除当前对象
  return RC::SUCCESS; // 返回成功
}

