/* Copyright (c) 2021-2022 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2024/02/04
//

#include "storage/clog/integrated_log_replayer.h"
#include "storage/clog/log_entry.h"

// 集成日志重放器构造函数
IntegratedLogReplayer::IntegratedLogReplayer(BufferPoolManager &bpm)
    : buffer_pool_log_replayer_(bpm), // 初始化缓冲池日志重放器
      record_log_replayer_(bpm), // 初始化记录管理日志重放器
      bplus_tree_log_replayer_(bpm), // 初始化 B+ 树日志重放器
      trx_log_replayer_(nullptr) // 初始化事务日志重放器为 nullptr
{}

// 带事务日志重放器的构造函数
IntegratedLogReplayer::IntegratedLogReplayer(BufferPoolManager &bpm, unique_ptr<LogReplayer> trx_log_replayer)
    : buffer_pool_log_replayer_(bpm), // 初始化缓冲池日志重放器
      record_log_replayer_(bpm), // 初始化记录管理日志重放器
      bplus_tree_log_replayer_(bpm), // 初始化 B+ 树日志重放器
      trx_log_replayer_(std::move(trx_log_replayer)) // 移动构造事务日志重放器
{}

// 重放日志条目
RC IntegratedLogReplayer::replay(const LogEntry &entry)
{
  // 根据日志条目的模块 ID 调用相应的重放器
  switch (entry.module().id()) {
    case LogModule::Id::BUFFER_POOL: return buffer_pool_log_replayer_.replay(entry); // 缓冲池重放
    case LogModule::Id::RECORD_MANAGER: return record_log_replayer_.replay(entry); // 记录管理重放
    case LogModule::Id::BPLUS_TREE: return bplus_tree_log_replayer_.replay(entry); // B+ 树重放
    case LogModule::Id::TRANSACTION: return trx_log_replayer_->replay(entry); // 事务重放
    default: return RC::INVALID_ARGUMENT; // 无效参数
  }
}

// 完成重放操作
RC IntegratedLogReplayer::on_done()
{
  // 对每个重放器调用 on_done 方法，检查重放是否成功
  RC rc = buffer_pool_log_replayer_.on_done();
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to do buffer pool log replay. rc=%s", strrc(rc)); // 记录缓冲池重放失败信息
    return rc;
  }
  
  rc = record_log_replayer_.on_done();
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to do record log replay. rc=%s", strrc(rc)); // 记录记录管理重放失败信息
    return rc;
  }

  rc = bplus_tree_log_replayer_.on_done();
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to do bplus tree log replay. rc=%s", strrc(rc)); // 记录 B+ 树重放失败信息
    return rc;
  }

  rc = trx_log_replayer_->on_done();
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to do mvcc trx log replay. rc=%s", strrc(rc)); // 记录事务重放失败信息
    return rc;
  }

  return RC::SUCCESS; // 返回成功
}
