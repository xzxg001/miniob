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

#include <sstream>
#include "storage/clog/log_entry.h"
#include "common/log/log.h"

////////////////////////////////////////////////////////////////////////////////
// struct LogHeader

const int32_t LogHeader::SIZE = sizeof(LogHeader); // 日志头的大小

// 将日志头信息转换为字符串格式
string LogHeader::to_string() const
{
  stringstream ss; // 使用字符串流
  ss << "lsn=" << lsn // 添加日志序列号
     << ", size=" << size // 添加日志大小
     << ", module_id=" << module_id << ":" << LogModule(module_id).name(); // 添加模块 ID 及名称

  return ss.str(); // 返回字符串
}

////////////////////////////////////////////////////////////////////////////////
// class LogEntry

LogEntry::LogEntry() // 默认构造函数
{
  header_.lsn = 0; // 初始化日志序列号为 0
  header_.size = 0; // 初始化日志大小为 0
}

// 移动构造函数
LogEntry::LogEntry(LogEntry &&other)
{
  header_ = other.header_; // 复制日志头
  data_ = std::move(other.data_); // 移动数据

  other.header_.lsn = 0; // 将源对象的日志序列号置为 0
  other.header_.size = 0; // 将源对象的日志大小置为 0
}

// 移动赋值运算符
LogEntry &LogEntry::operator=(LogEntry &&other)
{
  if (this == &other) { // 自我赋值检查
    return *this; // 返回自身
  }

  header_ = other.header_; // 复制日志头
  data_ = std::move(other.data_); // 移动数据

  other.header_.lsn = 0; // 将源对象的日志序列号置为 0
  other.header_.size = 0; // 将源对象的日志大小置为 0

  return *this; // 返回自身
}

// 初始化日志条目
RC LogEntry::init(LSN lsn, LogModule::Id module_id, vector<char> &&data)
{
  return init(lsn, LogModule(module_id), std::move(data)); // 调用重载的 init 函数
}

// 初始化日志条目（使用模块对象）
RC LogEntry::init(LSN lsn, LogModule module, vector<char> &&data)
{
  if (static_cast<int32_t>(data.size()) > max_payload_size()) { // 检查数据大小是否超过最大负载大小
    LOG_DEBUG("log entry size is too large. size=%d, max_payload_size=%d", data.size(), max_payload_size()); // 记录调试信息
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  header_.lsn = lsn; // 设置日志序列号
  header_.module_id = module.index(); // 设置模块 ID
  header_.size = static_cast<int32_t>(data.size()); // 设置日志大小
  data_ = std::move(data); // 移动数据
  return RC::SUCCESS; // 返回成功
}

// 将日志条目转换为字符串格式
string LogEntry::to_string() const
{
  return header_.to_string(); // 返回日志头的字符串表示
}

