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
// Created by Wangyunlai on 2023/06/16.
//

// 包含必要的头文件
#include <algorithm>
#ifdef __MUSL__
#include <errno.h>
#else
#include <sys/errno.h>
#endif
#include <unistd.h>

#include "net/buffered_writer.h"

// BufferedWriter类构造函数，初始化文件描述符和缓冲区
BufferedWriter::BufferedWriter(int fd) : fd_(fd), buffer_() {}
BufferedWriter::BufferedWriter(int fd, int32_t size) : fd_(fd), buffer_(size) {}

// 析构函数，确保在对象销毁时关闭文件描述符
BufferedWriter::~BufferedWriter() { close(); }

// 关闭文件描述符，并清空缓冲区
RC BufferedWriter::close()
{
  if (fd_ < 0) {
    return RC::SUCCESS; // 文件描述符已经关闭或无效
  }

  RC rc = flush(); // 尝试清空缓冲区
  if (OB_FAIL(rc)) {
    return rc; // 如果失败，返回错误码
  }

  fd_ = -1; // 标记文件描述符为无效

  return RC::SUCCESS;
}

// 写入数据到缓冲区，如果缓冲区满了则尝试flush
RC BufferedWriter::write(const char *data, int32_t size, int32_t &write_size)
{
  if (fd_ < 0) {
    return RC::INVALID_ARGUMENT; // 无效的文件描述符
  }

  if (buffer_.remain() == 0) { // 如果缓冲区没有空间了
    RC rc = flush_internal(size); // 尝试flush
    if (OB_FAIL(rc)) {
      return rc; // 如果失败，返回错误码
    }
  }

  return buffer_.write(data, size, write_size); // 写入数据到缓冲区
}

// 确保所有数据都写入到缓冲区，如果缓冲区满了则尝试flush
RC BufferedWriter::writen(const char *data, int32_t size)
{
  if (fd_ < 0) {
    return RC::INVALID_ARGUMENT; // 无效的文件描述符
  }

  int32_t write_size = 0; // 已写入的数据大小
  while (write_size < size) {
    int32_t tmp_write_size = 0;

    RC rc = write(data + write_size, size - write_size, tmp_write_size); // 写入数据
    if (OB_FAIL(rc)) {
      return rc; // 如果失败，返回错误码
    }

    write_size += tmp_write_size; // 更新已写入的数据大小
  }

  return RC::SUCCESS; // 成功写入所有数据
}

// 清空缓冲区，将数据写入到文件描述符
RC BufferedWriter::flush()
{
  if (fd_ < 0) {
    return RC::INVALID_ARGUMENT; // 无效的文件描述符
  }

  RC rc = RC::SUCCESS; // 初始化返回码为成功
  while (OB_SUCC(rc) && buffer_.size() > 0) { // 如果缓冲区有数据
    rc = flush_internal(buffer_.size()); // 尝试flush
  }
  return rc; // 返回最终的返回码
}

// 内部方法，用于将指定大小的数据从缓冲区写入到文件描述符
RC BufferedWriter::flush_internal(int32_t size)
{
  if (fd_ < 0) {
    return RC::INVALID_ARGUMENT; // 无效的文件描述符
  }

  RC rc = RC::SUCCESS; // 初始化返回码为成功

  int32_t write_size = 0; // 已写入的数据大小
  while (OB_SUCC(rc) && buffer_.size() > 0 && size > write_size) {
    const char *buf = nullptr; // 指向缓冲区的指针
    int32_t read_size = 0; // 从缓冲区读取的数据大小
    rc = buffer_.buffer(buf, read_size); // 获取缓冲区的数据
    if (OB_FAIL(rc)) {
      return rc; // 如果失败，返回错误码
    }

    ssize_t tmp_write_size = 0; // 临时写入大小
    while (tmp_write_size == 0) { // 如果写入大小为0，尝试重新写入
      tmp_write_size = ::write(fd_, buf, read_size); // 写入数据到文件描述符
      if (tmp_write_size < 0) {
        if (errno == EAGAIN || errno == EINTR) { // 如果是临时错误，忽略
          tmp_write_size = 0;
          continue;
        } else {
          return RC::IOERR_WRITE; // 其他错误，返回写入错误码
        }
      }
    }

    write_size += tmp_write_size; // 更新已写入的数据大小
    buffer_.forward(tmp_write_size); // 移动缓冲区的读写指针
  }

  return rc; // 返回最终的返回码
}