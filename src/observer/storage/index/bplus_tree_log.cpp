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
// Created by wangyunlai.wyl on 2024/02/05.
// 

#include "common/log/log.h"
#include "common/lang/defer.h"
#include "common/lang/algorithm.h"
#include "common/lang/sstream.h"
#include "storage/index/bplus_tree_log.h"
#include "storage/index/bplus_tree.h"
#include "storage/clog/log_handler.h"
#include "storage/clog/log_entry.h"
#include "storage/index/bplus_tree_log_entry.h"
#include "common/lang/serializer.h"
#include "storage/clog/vacuous_log_handler.h"

using namespace common;
using namespace bplus_tree;

///////////////////////////////////////////////////////////////////////////////
// 类 BplusTreeLogger
BplusTreeLogger::BplusTreeLogger(LogHandler &log_handler, int32_t buffer_pool_id)
    : log_handler_(log_handler), buffer_pool_id_(buffer_pool_id) {}

BplusTreeLogger::~BplusTreeLogger() { commit(); } // 析构时提交日志

RC BplusTreeLogger::init_header_page(Frame *frame, const IndexFileHeader &header)
{
  // 初始化文件头页面
  return append_log_entry(make_unique<InitHeaderPageLogEntryHandler>(frame, header));
}

RC BplusTreeLogger::update_root_page(Frame *frame, PageNum root_page_num, PageNum old_page_num)
{
  // 更新根页面
  return append_log_entry(make_unique<UpdateRootPageLogEntryHandler>(frame, root_page_num, old_page_num));
}

RC BplusTreeLogger::leaf_init_empty(IndexNodeHandler &node_handler)
{
  // 初始化叶子节点为空
  return append_log_entry(make_unique<LeafInitEmptyLogEntryHandler>(node_handler.frame()));
}

RC BplusTreeLogger::node_insert_items(IndexNodeHandler &node_handler, int index, span<const char> items, int item_num)
{
  // 在节点中插入项
  return append_log_entry(make_unique<NormalOperationLogEntryHandler>(
      node_handler.frame(), LogOperation::Type::NODE_INSERT, index, items, item_num));
}

RC BplusTreeLogger::node_remove_items(IndexNodeHandler &node_handler, int index, span<const char> items, int item_num)
{
  // 从节点中移除项
  return append_log_entry(make_unique<NormalOperationLogEntryHandler>(
      node_handler.frame(), LogOperation::Type::NODE_REMOVE, index, items, item_num));
}

RC BplusTreeLogger::leaf_set_next_page(IndexNodeHandler &node_handler, PageNum page_num, PageNum old_page_num)
{
  // 设置叶子节点的下一个页面
  return append_log_entry(make_unique<LeafSetNextPageLogEntryHandler>(node_handler.frame(), page_num, old_page_num));
}

RC BplusTreeLogger::internal_init_empty(IndexNodeHandler &node_handler)
{
  // 初始化内部节点为空
  return append_log_entry(make_unique<InternalInitEmptyLogEntryHandler>(node_handler.frame()));
}

RC BplusTreeLogger::internal_create_new_root(
    IndexNodeHandler &node_handler, PageNum first_page_num, span<const char> key, PageNum page_num)
{
  // 创建新的内部根节点
  return append_log_entry(
      make_unique<InternalCreateNewRootLogEntryHandler>(node_handler.frame(), first_page_num, key, page_num));
}

RC BplusTreeLogger::internal_update_key(
    IndexNodeHandler &node_handler, int index, span<const char> key, span<const char> old_key)
{
  // 更新内部节点的键
  return append_log_entry(make_unique<InternalUpdateKeyLogEntryHandler>(node_handler.frame(), index, key, old_key));
}

RC BplusTreeLogger::set_parent_page(IndexNodeHandler &node_handler, PageNum page_num, PageNum old_page_num)
{
  // 设置父页面
  return append_log_entry(make_unique<SetParentPageLogEntryHandler>(node_handler.frame(), page_num, old_page_num));
}

RC BplusTreeLogger::append_log_entry(unique_ptr<bplus_tree::LogEntryHandler> entry)
{
  // 添加日志条目
  if (!need_log_) {
    return RC::SUCCESS;
  }

  entries_.push_back(std::move(entry)); // 将条目加入列表
  return RC::SUCCESS;
}

RC BplusTreeLogger::commit()
{
  // 提交日志条目
  if (entries_.empty()) {
    return RC::SUCCESS;
  }

  LSN        lsn = 0; // 日志序列号
  Serializer buffer; // 序列化缓冲区
  buffer.write_int32(buffer_pool_id_); // 写入缓冲池ID

  for (auto &entry : entries_) {
    entry->serialize(buffer); // 序列化每个条目
  }

  Serializer::BufferType &buffer_data = buffer.data(); // 获取缓冲区数据

  RC rc = log_handler_.append(lsn, LogModule::Id::BPLUS_TREE, std::move(buffer_data)); // 写入日志
  if (RC::SUCCESS != rc) {
    LOG_WARN("failed to append log entry. rc=%s", strrc(rc));
    return rc; // 处理失败
  }

  for (auto &entry : entries_) {
    entry->frame()->set_lsn(lsn); // 设置页面的LSN
  }

  entries_.clear(); // 清空条目列表
  return RC::SUCCESS;
}

RC BplusTreeLogger::rollback(BplusTreeMiniTransaction &mtr, BplusTreeHandler &tree_handler)
{
  // 回滚操作
  need_log_ = false; // 禁用日志

  for (auto iter = entries_.rbegin(), itend = entries_.rend(); iter != itend; ++iter) {
    auto &entry = *iter;
    entry->rollback(mtr, tree_handler); // 执行回滚
  }

  entries_.clear(); // 清空条目列表
  need_log_ = true; // 恢复日志
  return RC::SUCCESS;
}

RC BplusTreeLogger::redo(BufferPoolManager &bpm, const LogEntry &entry)
{
  // 重做日志操作
  ASSERT(entry.module().id() == LogModule::Id::BPLUS_TREE, "invalid log entry: %s", entry.to_string().c_str());

  Deserializer buffer(entry.data(), entry.payload_size()); // 反序列化日志
  int32_t      buffer_pool_id = -1;
  int          ret            = buffer.read_int32(buffer_pool_id); // 读取缓冲池ID
  if (ret != 0) {
    LOG_ERROR("failed to read buffer pool id. ret=%d", ret);
    return RC::IOERR_READ;
  }

  DiskBufferPool *buffer_pool = nullptr;
  RC              rc          = bpm.get_buffer_pool(buffer_pool_id, buffer_pool); // 获取缓冲池
  if (OB_FAIL(rc) || buffer_pool == nullptr) {
    LOG_WARN("failed to get buffer pool. rc=%s, buffer_pool_id=%d", strrc(rc), buffer_pool_id);
    return rc;
  }

  VacuousLogHandler log_handler; // 创建空日志处理器
  BplusTreeHandler  tree_handler; // 创建B+树处理器
  rc = tree_handler.open(log_handler, *buffer_pool); // 打开树处理器
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open bplus tree handler. rc=%s", strrc(rc));
    return rc;
  }

  BplusTreeMiniTransaction mtr(tree_handler); // 创建事务
  rc = mtr.logger().__redo(entry.lsn(), mtr, tree_handler, buffer); // 重做操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to redo log entry. rc=%s", strrc(rc));
    return rc;
  }

  ASSERT(mtr.logger().entries_.empty(), "entries should be empty after redo"); // 确保条目为空
  return rc;
}

RC BplusTreeLogger::__redo(LSN lsn, BplusTreeMiniTransaction &mtr, BplusTreeHandler &tree_handler, Deserializer &redo_buffer)
{
  // 重做内部操作
  need_log_ = false; // 禁用日志

  DEFER(need_log_ = true); // 确保在退出时恢复日志状态

  RC rc = RC::SUCCESS;
  vector<Frame *> frames; // 用于存储帧的列表
  while (redo_buffer.remain() > 0) {
    unique_ptr<LogEntryHandler> entry;

    rc = LogEntryHandler::from_buffer(tree_handler.buffer_pool(), redo_buffer, entry); // 从缓冲区反序列化条目
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to deserialize log entry. rc=%s", strrc(rc));
      break; // 处理失败，退出循环
    }
    Frame *frame = entry->frame(); // 获取帧
    if (frame != nullptr) {
      if (frame->lsn() >= lsn) {
        LOG_TRACE("no need to redo. frame=%p:%s, redo lsn=%ld", frame, frame->to_string().c_str(), lsn);
        frame->unpin(); // 解除引用
        continue; // 不需要重做，继续下一个条目
      } else {
        frames.push_back(frame); // 添加到帧列表
      }
    } else {
      LOG_TRACE("frame is null, skip the redo action");
      continue; // 如果帧为空，跳过
    }

    rc = entry->redo(mtr, tree_handler); // 执行重做操作
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to redo log entry. rc=%s, lsn=%d, entry=%s", strrc(rc), lsn, entry->to_string().c_str());
      break; // 处理失败，退出循环
    }
  }

  if (OB_SUCC(rc)) {
    for (Frame *frame : frames) {
      frame->set_lsn(lsn); // 设置帧的LSN
      frame->unpin(); // 解除引用
    }
  }

  return RC::SUCCESS;
}

string BplusTreeLogger::log_entry_to_string(const LogEntry &entry)
{
  // 将日志条目转换为字符串
  stringstream ss;
  Deserializer buffer(entry.data(), entry.payload_size()); // 反序列化日志
  int32_t      buffer_pool_id = -1;
  int          ret            = buffer.read_int32(buffer_pool_id); // 读取缓冲池ID
  if (ret != 0) {
    LOG_ERROR("failed to read buffer pool id. ret=%d", ret);
    return ss.str(); // 处理失败，返回空字符串
  }

  ss << "buffer_pool_id:" << buffer_pool_id; // 添加缓冲池ID到字符串
  while (buffer.remain() > 0) {
    unique_ptr<LogEntryHandler> entry;

    RC rc = LogEntryHandler::from_buffer(buffer, entry); // 从缓冲区反序列化条目
    if (RC::SUCCESS != rc) {
      LOG_WARN("failed to deserialize log entry. rc=%s", strrc(rc));
      return ss.str(); // 处理失败，返回当前字符串
    }

    ss << ","; // 添加分隔符
    ss << entry->to_string(); // 添加条目的字符串表示
  }
  return ss.str(); // 返回完整字符串
}

///////////////////////////////////////////////////////////////////////////////
// 类 BplusTreeMiniTransaction
BplusTreeMiniTransaction::BplusTreeMiniTransaction(BplusTreeHandler &tree_handler, RC *operation_result /* =nullptr */)
    : tree_handler_(tree_handler),
      operation_result_(operation_result),
      latch_memo_(&tree_handler.buffer_pool()),
      logger_(tree_handler.log_handler(), tree_handler.buffer_pool().id()) {}

BplusTreeMiniTransaction::~BplusTreeMiniTransaction()
{
  // 析构函数，根据操作结果决定提交或回滚
  if (nullptr == operation_result_) {
    return; // 如果操作结果为空，直接返回
  }
  
  if (OB_SUCC(*operation_result_)) {
    commit(); // 提交事务
  } else {
    rollback(); // 回滚事务
  }
}

RC BplusTreeMiniTransaction::commit() { return logger_.commit(); } // 提交操作

RC BplusTreeMiniTransaction::rollback() { return logger_.rollback(*this, tree_handler_); } // 回滚操作

///////////////////////////////////////////////////////////////////////////////
// 类 BplusTreeLogReplayer
BplusTreeLogReplayer::BplusTreeLogReplayer(BufferPoolManager &bpm) : buffer_pool_manager_(bpm) {}

RC BplusTreeLogReplayer::replay(const LogEntry &entry) { return BplusTreeLogger::redo(buffer_pool_manager_, entry); } // 重放日志操作
