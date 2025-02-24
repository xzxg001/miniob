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
// Created by wangyunlai on 2024/01/31
//

#include "storage/clog/log_buffer.h"
#include "storage/clog/log_file.h"
#include "common/lang/chrono.h"

using namespace common;

// 日志条目缓冲区初始化
RC LogEntryBuffer::init(LSN lsn, int32_t max_bytes /*= 0*/)
{
  current_lsn_.store(lsn); // 当前日志序列号初始化
  flushed_lsn_.store(lsn); // 已刷新的日志序列号初始化

  if (max_bytes > 0) {
    max_bytes_ = max_bytes; // 设置最大字节数
  }
  return RC::SUCCESS; // 返回成功
}

// 向缓冲区添加日志条目（使用模块 ID）
RC LogEntryBuffer::append(LSN &lsn, LogModule::Id module_id, vector<char> &&data)
{
  return append(lsn, LogModule(module_id), std::move(data)); // 调用重载的 append 函数
}

// 向缓冲区添加日志条目（使用模块对象）
RC LogEntryBuffer::append(LSN &lsn, LogModule module, vector<char> &&data)
{
  // 控制当前缓冲区使用的内存
  // 如果当前想要新插入的日志比较大，不会做控制，所以理论上容纳的最大缓冲区内存是 2 * max_bytes_
  while (bytes_.load() >= max_bytes_) {
    this_thread::sleep_for(chrono::milliseconds(10)); // 如果缓冲区已满，则强制等待
  }

  LogEntry entry; // 创建日志条目
  RC rc = entry.init(lsn, module, std::move(data)); // 初始化日志条目
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to init log entry. rc=%s", strrc(rc)); // 初始化失败，记录警告
    return rc; // 返回错误代码
  }

  lock_guard guard(mutex_); // 锁定互斥量以保证线程安全
  lsn = ++current_lsn_; // 增加当前日志序列号并更新传入参数
  entry.set_lsn(lsn); // 设置日志条目的序列号

  entries_.push_back(std::move(entry)); // 将日志条目添加到缓冲区
  bytes_ += entry.total_size(); // 更新当前使用的字节数
  return RC::SUCCESS; // 返回成功
}

// 刷新缓冲区中的日志条目到日志文件
RC LogEntryBuffer::flush(LogFileWriter &writer, int &count)
{
  count = 0; // 计数器初始化

  // 当缓冲区中还有日志条目时
  while (entry_number() > 0) {
    LogEntry entry; // 创建日志条目

    {
      lock_guard guard(mutex_); // 锁定互斥量以保证线程安全
      if (entries_.empty()) {
        break; // 如果缓冲区为空，退出循环
      }

      LogEntry &front_entry = entries_.front(); // 获取队首日志条目
      ASSERT(front_entry.lsn() > 0 && front_entry.payload_size() > 0, "invalid log entry"); // 断言日志条目有效
      entry = std::move(entries_.front()); // 移动队首条目
      ASSERT(entry.payload_size() > 0 && entry.lsn() > 0, "invalid log entry"); // 断言日志条目有效
      entries_.pop_front(); // 从缓冲区移除条目
      bytes_ -= entry.total_size(); // 更新当前使用的字节数
    }
    
    RC rc = writer.write(entry); // 将条目写入日志文件
    if (OB_FAIL(rc)) {
      lock_guard guard(mutex_); // 锁定互斥量以保证线程安全
      entries_.emplace_front(std::move(entry)); // 写入失败，将条目放回缓冲区
      LogEntry &front_entry = entries_.front();
      ASSERT(front_entry.lsn() > 0 && front_entry.payload_size() > 0, "invalid log entry");
      return rc; // 返回错误代码
    } else {
      ++count; // 成功写入，计数器加一
      flushed_lsn_ = entry.lsn(); // 更新已刷新日志序列号
    }
  }
  
  return RC::SUCCESS; // 返回成功
}

// 获取当前缓冲区占用的字节数
int64_t LogEntryBuffer::bytes() const
{
  return bytes_.load(); // 返回当前使用的字节数
}

// 获取当前缓冲区中的日志条目数量
int32_t LogEntryBuffer::entry_number() const
{
  return entries_.size(); // 返回条目数量
}
