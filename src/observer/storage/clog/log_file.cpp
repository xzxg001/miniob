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

#include <fcntl.h>

#include "common/lang/string_view.h"
#include "common/lang/charconv.h"
#include "common/log/log.h"
#include "storage/clog/log_file.h"
#include "storage/clog/log_entry.h"
#include "common/io/io.h"

using namespace common;

// 打开日志文件
RC LogFileReader::open(const char *filename)
{
  filename_ = filename;

  fd_ = ::open(filename, O_RDONLY); // 以只读方式打开文件
  if (fd_ < 0) {
    LOG_WARN("open file failed. filename=%s, error=%s", filename, strerror(errno)); // 记录打开文件失败的警告
    return RC::FILE_OPEN;
  }

  LOG_INFO("open file success. filename=%s, fd=%d", filename, fd_); // 记录打开文件成功的信息
  return RC::SUCCESS;
}

// 关闭日志文件
RC LogFileReader::close()
{
  if (fd_ < 0) {
    return RC::FILE_NOT_OPENED; // 文件未打开，返回错误
  }

  ::close(fd_); // 关闭文件描述符
  fd_ = -1; // 重置文件描述符
  return RC::SUCCESS;
}

// 遍历日志文件中的条目
RC LogFileReader::iterate(function<RC(LogEntry &)> callback, LSN start_lsn /*=0*/)
{
  RC rc = skip_to(start_lsn); // 跳过到指定的日志序列号
  if (OB_FAIL(rc)) {
    return rc;
  }

  LogHeader header; // 日志头部
  while (true) {
    int ret = readn(fd_, reinterpret_cast<char *>(&header), LogHeader::SIZE); // 读取日志头
    if (0 != ret) {
      if (-1 == ret) {
        // 到达文件结尾
        break;
      }
      LOG_WARN("read file failed. filename=%s, ret = %d, error=%s", filename_.c_str(), ret, strerror(errno)); // 记录读取文件失败的警告
      return RC::IOERR_READ; // 返回读取错误
    }

    // 检查日志条目大小是否合法
    if (header.size < 0 || header.size > LogEntry::max_payload_size()) {
      LOG_WARN("invalid log entry size. filename=%s, size=%d", filename_.c_str(), header.size);
      return RC::IOERR_READ;
    }

    vector<char> data(header.size); // 创建数据缓存
    ret = readn(fd_, data.data(), header.size); // 读取日志条目的数据
    if (0 != ret) {
      LOG_WARN("read file failed. filename=%s, size=%d, ret=%d, error=%s", filename_.c_str(), header.size, ret, strerror(errno)); // 记录读取失败的警告
      return RC::IOERR_READ; // 返回读取错误
    }

    LogEntry entry; // 创建日志条目
    entry.init(header.lsn, LogModule(header.module_id), std::move(data)); // 初始化日志条目
    rc = callback(entry); // 调用回调处理日志条目
    if (OB_FAIL(rc)) {
      LOG_INFO("iterate log entry failed. entry=%s, rc=%s", entry.to_string().c_str(), strrc(rc)); // 记录迭代日志条目失败的信息
      return rc;
    }
    LOG_TRACE("redo log iterate entry success. entry=%s", entry.to_string().c_str()); // 记录迭代成功的信息
  }

  return RC::SUCCESS; // 返回成功
}

// 跳过到指定的日志序列号
RC LogFileReader::skip_to(LSN start_lsn)
{
  if (fd_ < 0) {
    return RC::FILE_NOT_OPENED; // 文件未打开，返回错误
  }

  off_t pos = lseek(fd_, 0, SEEK_SET); // 移动文件指针到文件开头
  if (off_t(-1) == pos) {
    LOG_WARN("seek file failed. seek to the beginning. filename=%s, error=%s", filename_.c_str(), strerror(errno)); // 记录移动文件指针失败的警告
    return RC::IOERR_SEEK; // 返回寻址错误
  }

  LogHeader header; // 日志头部
  while (true) {
    int ret = readn(fd_, reinterpret_cast<char *>(&header), LogHeader::SIZE); // 读取日志头
    if (0 != ret) {
      if (-1 == ret) {
        // 到达文件结尾
        break;
      }
      LOG_WARN("read file failed. filename=%s, ret = %d, error=%s", filename_.c_str(), ret, strerror(errno)); // 记录读取文件失败的警告
      return RC::IOERR_READ; // 返回读取错误
    }

    // 如果当前日志序列号大于等于起始序列号，则跳过当前日志头
    if (header.lsn >= start_lsn) {
      off_t pos = lseek(fd_, -LogHeader::SIZE, SEEK_CUR); // 移动文件指针回退到当前日志头
      if (off_t(-1) == pos) {
        LOG_WARN("seek file failed. skip back log header. filename=%s, error=%s", filename_.c_str(), strerror(errno)); // 记录移动文件指针失败的警告
        return RC::IOERR_SEEK; // 返回寻址错误
      }
      break;
    }

    // 检查日志条目大小是否合法
    if (header.size < 0 || header.size > LogEntry::max_payload_size()) {
      LOG_WARN("invalid log entry size. filename=%s, size=%d", filename_.c_str(), header.size);
      return RC::IOERR_READ; // 返回读取错误
    }

    // 移动文件指针跳过当前日志条目
    pos = lseek(fd_, header.size, SEEK_CUR);
    if (off_t(-1) == pos) {
      LOG_WARN("seek file failed. skip log entry payload. filename=%s, error=%s", filename_.c_str(), strerror(errno)); // 记录移动文件指针失败的警告
      return RC::IOERR_SEEK; // 返回寻址错误
    }
  }

  return RC::SUCCESS; // 返回成功
}

////////////////////////////////////////////////////////////////////////////////
// LogFileWriter

LogFileWriter::~LogFileWriter()
{
  (void)this->close(); // 析构函数中关闭文件
}

// 打开日志文件进行写入
RC LogFileWriter::open(const char *filename, int end_lsn)
{
  if (fd_ >= 0) {
    return RC::FILE_OPEN; // 文件已打开，返回错误
  }

  filename_ = filename; // 保存文件名
  end_lsn_ = end_lsn; // 保存结束序列号

  fd_ = ::open(filename, O_WRONLY | O_APPEND | O_CREAT | O_SYNC, 0644); // 以写入附加的方式打开文件
  if (fd_ < 0) {
    LOG_WARN("open file failed. filename=%s, error=%s", filename, strerror(errno)); // 记录打开文件失败的警告
    return RC::FILE_OPEN; // 返回打开文件错误
  }

  LOG_INFO("open file success. filename=%s, fd=%d", filename, fd_); // 记录打开文件成功的信息
  return RC::SUCCESS; // 返回成功
}

// 关闭日志文件
RC LogFileWriter::close()
{
  if (fd_ < 0) {
    return RC::FILE_NOT_OPENED; // 文件未打开，返回错误
  }

  ::close(fd_); // 关闭文件描述符
  fd_ = -1; // 重置文件描述符
  return RC::SUCCESS; // 返回成功
}

// 写入日志条目
RC LogFileWriter::write(LogEntry &entry)
{
  // 一个日志文件写的日志条数是有限制的
  if (entry.lsn() > end_lsn_) {
    return RC::LOG_FILE_FULL; // 超出日志文件的最大序列号，返回错误
  }

  if (fd_ < 0) {
    return RC::FILE_NOT_OPENED; // 文件未打开，返回错误
  }

  if (entry.lsn() <= last_lsn_) {
    LOG_WARN("write log entry failed. lsn is too small. filename=%s, last_lsn=%ld, entry=%s", 
             filename_.c_str(), last_lsn_, entry.to_string().c_str()); // 记录日志条目写入失败的警告
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  // WARNING: 需要处理日志写一半的情况
  // 日志只写成功一部分到文件中非常难处理
  int ret = writen(fd_, reinterpret_cast<const char *>(&entry.header()), LogHeader::SIZE); // 写入日志头
  if (0 != ret) {
    LOG_WARN("write log entry header failed. filename=%s, ret = %d, error=%s, entry=%s", 
             filename_.c_str(), ret, strerror(errno), entry.to_string().c_str()); // 记录写入日志头失败的警告
    return RC::IOERR_WRITE; // 返回写入错误
  }

  ret = writen(fd_, entry.data(), entry.payload_size()); // 写入日志数据
  if (0 != ret) {
    LOG_WARN("write log entry payload failed. filename=%s, ret = %d, error=%s, entry=%s", 
             filename_.c_str(), ret, strerror(errno), entry.to_string().c_str()); // 记录写入日志数据失败的警告
    return RC::IOERR_WRITE; // 返回写入错误
  }

  last_lsn_ = entry.lsn(); // 更新最后写入的序列号
  LOG_TRACE("write log entry success. filename=%s, entry=%s", filename_.c_str(), entry.to_string().c_str()); // 记录写入成功的信息
  return RC::SUCCESS; // 返回成功
}

// 检查文件是否有效
bool LogFileWriter::valid() const
{
  return fd_ >= 0; // 如果文件描述符大于等于 0，则文件有效
}

// 检查文件是否已满
bool LogFileWriter::full() const
{
  return last_lsn_ >= end_lsn_; // 如果最后写入的序列号大于等于结束序列号，则文件已满
}

// 返回文件名
string LogFileWriter::to_string() const
{
  return filename_; // 返回文件名
}

////////////////////////////////////////////////////////////////////////////////
// LogFileManager

// 初始化日志文件管理器
RC LogFileManager::init(const char *directory, int max_entry_number_per_file)
{
  directory_ = filesystem::absolute(filesystem::path(directory)); // 处理绝对路径
  max_entry_number_per_file_ = max_entry_number_per_file; // 保存每个文件的最大条目数

  // 检查目录是否存在，不存在则创建
  if (!filesystem::is_directory(directory_)) {
    LOG_INFO("directory is not exist. directory=%s", directory_.c_str());

    error_code ec;
    bool ret = filesystem::create_directories(directory_, ec); // 创建目录
    if (!ret) {
      LOG_WARN("create directory failed. directory=%s, error=%s", directory_.c_str(), ec.message().c_str()); // 记录创建目录失败的警告
      return RC::FILE_CREATE; // 返回创建文件夹错误
    }
  }

  // 列出所有的日志文件
  for (const filesystem::directory_entry &dir_entry : filesystem::directory_iterator(directory_)) {
    if (!dir_entry.is_regular_file()) {
      continue; // 跳过非普通文件
    }

    string filename = dir_entry.path().filename().string(); // 获取文件名
    LSN lsn = 0; // 初始化日志序列号
    RC rc = get_lsn_from_filename(filename, lsn); // 从文件名中获取序列号
    if (OB_FAIL(rc)) {
      LOG_TRACE("invalid log file name. filename=%s", filename.c_str()); // 记录无效文件名的追踪信息
      continue; // 跳过无效文件
    }

    if (log_files_.find(lsn) != log_files_.end()) {
      LOG_TRACE("duplicate log file. filename1=%s, filename2=%s", 
                filename.c_str(), log_files_.find(lsn)->second.filename().c_str()); // 记录重复日志文件的追踪信息
      continue; // 跳过重复文件
    }

    log_files_.emplace(lsn, dir_entry.path()); // 添加日志文件到管理器
  }

  LOG_INFO("init log file manager success. directory=%s, log files=%d", 
           directory_.c_str(), static_cast<int>(log_files_.size())); // 记录初始化成功的信息
  return RC::SUCCESS; // 返回成功
}

// 从文件名中获取日志序列号
RC LogFileManager::get_lsn_from_filename(const string &filename, LSN &lsn)
{
  if (!filename.starts_with(file_prefix_) || !filename.ends_with(file_suffix_)) {
    return RC::INVALID_ARGUMENT; // 文件名格式不正确，返回无效参数错误
  }

  string_view lsn_str(filename.data() + strlen(file_prefix_), filename.length() - strlen(file_suffix_) - strlen(file_prefix_)); // 获取序列号字符串
  from_chars_result result = from_chars(lsn_str.data(), lsn_str.data() + lsn_str.size(), lsn); // 转换为序列号
  if (result.ec != errc()) {
    LOG_TRACE("invalid log file name: cannot calc lsn. filename=%s, error=%s", 
              filename.c_str(), strerror(static_cast<int>(result.ec))); // 记录转换错误的追踪信息
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  return RC::SUCCESS; // 返回成功
}

// 列出日志文件
RC LogFileManager::list_files(vector<string> &files, LSN start_lsn)
{
  files.clear(); // 清空文件列表

  // 找到比 start_lsn 相等或小的第一个日志文件
  for (auto &file : log_files_) {
    if (file.first + max_entry_number_per_file_ - 1 >= start_lsn) {
      files.emplace_back(file.second.string()); // 添加文件到列表
    }
  }

  return RC::SUCCESS; // 返回成功
}

// 获取最后一个日志文件
RC LogFileManager::last_file(LogFileWriter &file_writer)
{
  if (log_files_.empty()) {
    return next_file(file_writer); // 如果没有文件，返回下一个文件
  }

  file_writer.close(); // 关闭当前文件

  auto last_file_item = log_files_.rbegin(); // 获取最后一个日志文件
  return file_writer.open(last_file_item->second.c_str(), 
                          last_file_item->first + max_entry_number_per_file_ - 1); // 打开最后一个文件
}

// 获取下一个日志文件
RC LogFileManager::next_file(LogFileWriter &file_writer)
{
  file_writer.close(); // 关闭当前文件

  LSN lsn = 0; // 初始化日志序列号
  if (!log_files_.empty()) {
    lsn = log_files_.rbegin()->first + max_entry_number_per_file_; // 获取下一个日志序列号
  }

  string filename = file_prefix_ + std::to_string(lsn) + file_suffix_; // 构造文件名
  filesystem::path file_path = directory_ / filename; // 创建文件路径
  log_files_.emplace(lsn, file_path); // 添加日志文件到管理器

  return file_writer.open(file_path.c_str(), lsn + max_entry_number_per_file_ - 1); // 打开新日志文件
}

