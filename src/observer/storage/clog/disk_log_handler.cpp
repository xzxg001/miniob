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
// Created by wangyunlai on 2024/01/30
//

#include "common/thread/thread_util.h"
#include "storage/clog/disk_log_handler.h"
#include "storage/clog/log_file.h"
#include "storage/clog/log_replayer.h"
#include "common/lang/chrono.h"

using namespace common;

////////////////////////////////////////////////////////////////////////////////
// LogHandler

// 初始化 DiskLogHandler，设置日志文件路径和最大条目数
RC DiskLogHandler::init(const char *path)
{
  const int max_entry_number_per_file = 1000; // 每个文件的最大条目数
  return file_manager_.init(path, max_entry_number_per_file);
}

// 启动日志处理器线程
RC DiskLogHandler::start()
{
  if (thread_) {
    LOG_ERROR("log has been started"); // 如果线程已启动，记录错误信息
    return RC::INTERNAL;
  }

  running_.store(true); // 设置运行标志为 true
  thread_ = make_unique<thread>(&DiskLogHandler::thread_func, this); // 创建并启动线程
  LOG_INFO("log handler started"); // 记录日志处理器启动信息
  return RC::SUCCESS;
}

// 停止日志处理器线程
RC DiskLogHandler::stop()
{
  if (!thread_) {
    LOG_ERROR("log has not been started"); // 如果线程未启动，记录错误信息
    return RC::INTERNAL;
  }

  running_.store(false); // 设置运行标志为 false
  LOG_INFO("log handler stopped"); // 记录日志处理器停止信息
  return RC::SUCCESS;
}

// 等待线程终止
RC DiskLogHandler::await_termination()
{
  if (!thread_) {
    LOG_ERROR("log has not been started"); // 如果线程未启动，记录错误信息
    return RC::INTERNAL;
  }

  if (running_.load()) {
    LOG_ERROR("log handler is running"); // 如果线程仍在运行，记录错误信息
    return RC::INTERNAL;
  }

  thread_->join(); // 等待线程结束
  thread_.reset(); // 重置线程
  LOG_INFO("log handler joined"); // 记录日志处理器已合并信息
  return RC::SUCCESS;
}

// 重放日志
RC DiskLogHandler::replay(LogReplayer &replayer, LSN start_lsn)
{
  LSN max_lsn = 0; // 最大日志序列号
  // 定义重放回调函数
  auto replay_callback = [&replayer, &max_lsn](LogEntry &entry) -> RC {
    if (entry.lsn() > max_lsn) {
      max_lsn = entry.lsn(); // 更新最大 LSN
    }
    return replayer.replay(entry); // 调用重放函数
  };

  RC rc = iterate(replay_callback, start_lsn); // 迭代重放日志
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to iterate log entries. rc=%s", strrc(rc)); // 记录迭代日志失败信息
    return rc;
  }

  rc = entry_buffer_.init(max_lsn); // 初始化日志条目缓冲区
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to init log entry buffer. rc=%s", strrc(rc)); // 记录初始化失败信息
    return rc;
  }

  LOG_INFO("replay clog files done. start lsn=%ld, max_lsn=%ld", start_lsn, max_lsn); // 记录重放完成信息
  return rc;
}

// 迭代日志文件
RC DiskLogHandler::iterate(function<RC(LogEntry&)> consumer, LSN start_lsn)
{
  vector<string> log_files; // 存储日志文件列表
  RC rc = file_manager_.list_files(log_files, start_lsn); // 列出日志文件
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to list clog files. rc=%s", strrc(rc)); // 记录列出文件失败信息
    return rc;
  }

  for (auto &file : log_files) {
    LogFileReader file_handle; // 创建日志文件读取器
    rc = file_handle.open(file.c_str()); // 打开日志文件
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to open clog file. rc=%s, file=%s", strrc(rc), file.c_str()); // 记录打开文件失败信息
      return rc;
    }

    rc = file_handle.iterate(consumer, start_lsn); // 迭代日志文件
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to iterate clog file. rc=%s, file=%s", strrc(rc), file.c_str()); // 记录迭代失败信息
      return rc;
    }

    rc = file_handle.close(); // 关闭日志文件
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to close clog file. rc=%s, file=%s", strrc(rc), file.c_str()); // 记录关闭文件失败信息
      return rc;
    }
  }

  LOG_INFO("iterate clog files done. rc=%s", strrc(rc)); // 记录迭代完成信息
  return RC::SUCCESS;
}

// 追加日志条目
RC DiskLogHandler::_append(LSN &lsn, LogModule module, vector<char> &&data)
{
  ASSERT(running_.load(), "log handler is not running. lsn=%ld, module=%s, size=%d", 
        lsn, module.name(), data.size()); // 确保日志处理器正在运行

  RC rc = entry_buffer_.append(lsn, module, std::move(data)); // 追加日志条目到缓冲区
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to append log entry to buffer. rc=%s", strrc(rc)); // 记录追加失败信息
    return rc;
  }

  return RC::SUCCESS;
}

// 等待特定 LSN
RC DiskLogHandler::wait_lsn(LSN lsn)
{
  // 直接强制等待。在生产系统中，我们可能会使用条件变量来等待。
  while (running_.load() && current_flushed_lsn() < lsn) {
    this_thread::sleep_for(chrono::milliseconds(100)); // 每 100 毫秒检查一次
  }

  if (current_flushed_lsn() >= lsn) {
    return RC::SUCCESS; // 如果已达到或超过 LSN，返回成功
  } else {
    return RC::INTERNAL; // 否则返回内部错误
  }
}

// 日志处理线程函数
void DiskLogHandler::thread_func()
{
  /*
  这个线程一直不停的循环，检查日志缓冲区中是否有日志在内存中，如果在内存中就刷新到磁盘。
  这种做法很粗暴简单，与生产数据库的做法不同。生产数据库通常会在内存达到一定量，或者一定时间
  之后，会刷新一下磁盘。这样做的好处是减少磁盘的IO次数，提高性能。
  */
  thread_set_name("LogHandler"); // 设置线程名称
  LOG_INFO("log handler thread started"); // 记录日志处理线程启动信息

  LogFileWriter file_writer; // 创建日志文件写入器
  
  RC rc = RC::SUCCESS;
  while (running_.load() || entry_buffer_.entry_number() > 0) { // 当日志处理器正在运行或有条目待处理时循环
    if (!file_writer.valid() || rc == RC::LOG_FILE_FULL) {
      if (rc == RC::LOG_FILE_FULL) {
        // 我们在这里判断日志文件是否写满了。
        rc = file_manager_.next_file(file_writer); // 获取下一个日志文件
      } else {
        rc = file_manager_.last_file(file_writer); // 获取最后一个日志文件
      }
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to open log file. rc=%s", strrc(rc)); // 记录打开日志文件失败信息
        // 总是使用最简单的方法等待一段时间，期望错误会被修复。
        // 这在生产系统中是不被允许的。
        this_thread::sleep_for(chrono::milliseconds(100)); // 等待 100 毫秒
        continue;
      }
      LOG_INFO("open log file success. file=%s", file_writer.to_string().c_str()); // 记录成功打开文件信息
    }

    int flush_count = 0; // 刷新计数
    rc = entry_buffer_.flush(file_writer, flush_count); // 刷新日志条目缓冲区
    if (OB_FAIL(rc) && RC::LOG_FILE_FULL != rc) {
      LOG_WARN("failed to flush log entry buffer. rc=%s", strrc(rc)); // 记录刷新失败信息
    }

    if (flush_count == 0 && rc == RC::SUCCESS) {
      this_thread::sleep_for(chrono::milliseconds(100)); // 如果没有刷新条目，等待 100 毫秒
      continue;
    }
  }

  LOG_INFO("log handler thread stopped"); // 记录日志处理线程停止信息
}
