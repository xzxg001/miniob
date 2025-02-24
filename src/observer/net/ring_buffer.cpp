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
#include "common/lang/algorithm.h"  // 引入算法相关函数
#include "common/log/log.h"        // 引入日志记录功能
#include "net/ring_buffer.h"        // 引入RingBuffer类声明

const int32_t DEFAULT_BUFFER_SIZE = 16 * 1024;  // 默认缓冲区大小为16KB

// RingBuffer类的构造函数，初始化缓冲区大小为默认值
RingBuffer::RingBuffer() : RingBuffer(DEFAULT_BUFFER_SIZE) {}

// RingBuffer类的构造函数，初始化缓冲区大小为指定值
RingBuffer::RingBuffer(int32_t size) : buffer_(size) {}

// RingBuffer类的析构函数，目前为空实现
RingBuffer::~RingBuffer() {}

// 从环形缓冲区读取数据
RC RingBuffer::read(char *buf, int32_t size, int32_t &read_size) {
  if (size < 0) {  // 检查读取大小是否有效
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;  // 初始化返回码为成功
  read_size = 0;        // 初始化已读取大小为0
  while (OB_SUCC(rc) && read_size < size && this->size() > 0) {  // 循环直到读取完毕或缓冲区空
    const char *tmp_buf = nullptr;  // 临时缓冲区指针
    int32_t tmp_size = 0;           // 临时缓冲区大小
    rc = buffer(tmp_buf, tmp_size);  // 获取缓冲区中的数据
    if (OB_SUCC(rc)) {  // 如果获取成功
      int32_t copy_size = min(size - read_size, tmp_size);  // 计算本次读取大小
      memcpy(buf + read_size, tmp_buf, copy_size);  // 将数据复制到用户缓冲区
      read_size += copy_size;  // 更新已读取大小

      rc = forward(copy_size);  // 移动读指针
    }
  }

  return rc;  // 返回操作结果
}

// 获取环形缓冲区当前可读的数据
RC RingBuffer::buffer(const char *&buf, int32_t &read_size) {
  const int32_t size = this->size();  // 获取缓冲区中的数据量
  if (size == 0) {  // 如果缓冲区为空
    buf = buffer_.data();  // 将缓冲区指针设置为空
    read_size = 0;        // 设置可读大小为0
    return RC::SUCCESS;
  }

  const int32_t read_pos = this->read_pos();  // 获取读指针位置
  // 计算可读数据的大小
  if (read_pos < write_pos_) {
    read_size = write_pos_ - read_pos;
  } else {
    read_size = capacity() - read_pos;
  }
  buf = buffer_.data() + read_pos;  // 设置缓冲区指针
  return RC::SUCCESS;
}

// 移动环形缓冲区的读指针
RC RingBuffer::forward(int32_t size) {
  if (size <= 0) {  // 检查移动大小是否有效
    return RC::INVALID_ARGUMENT;
  }

  if (size > this->size()) {  // 检查移动大小是否超过缓冲区中的数据量
    LOG_DEBUG("forward size is too large. size=%d, size=%d", size, this->size());
    return RC::INVALID_ARGUMENT;
  }

  data_size_ -= size;  // 更新缓冲区中的数据量
  return RC::SUCCESS;  // 返回成功
}

// 向环形缓冲区写入数据
RC RingBuffer::write(const char *data, int32_t size, int32_t &write_size) {
  if (size < 0) {  // 检查写入大小是否有效
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;      // 初始化返回码为成功
  write_size = 0;           // 初始化已写入大小为0
  while (OB_SUCC(rc) && write_size < size && this->remain() > 0) {  // 循环直到写入完毕或缓冲区满
    const int32_t read_pos = this->read_pos();  // 获取读指针位置
    const int32_t tmp_buf_size = (read_pos <= write_pos_) ? (capacity() - write_pos_) : (read_pos - write_pos_);  // 计算可写入大小

    const int32_t copy_size = min(size - write_size, tmp_buf_size);  // 计算本次写入大小
    memcpy(buffer_.data() + write_pos_, data + write_size, copy_size);  // 将数据复制到缓冲区
    write_size += copy_size;  // 更新已写入大小
    write_pos_ = (write_pos_ + copy_size) % capacity();  // 更新写指针位置
    data_size_ += copy_size;  // 更新缓冲区中的数据量
  }

  return rc;  // 返回操作结果
}