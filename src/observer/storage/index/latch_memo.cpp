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
// Created by Wangyunlai on 2023/03/08.
// 

#include "storage/index/latch_memo.h"
#include "common/lang/mutex.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/buffer/frame.h"

// LatchMemoItem构造函数
LatchMemoItem::LatchMemoItem(LatchMemoType type, Frame *frame) {
  this->type  = type; // 设置锁类型
  this->frame = frame; // 设置帧
}

// 另一种构造函数，使用共享锁
LatchMemoItem::LatchMemoItem(LatchMemoType type, common::SharedMutex *lock) {
  this->type = type; // 设置锁类型
  this->lock = lock; // 设置共享锁
}

////////////////////////////////////////////////////////////////////////////////

// LatchMemo构造函数
LatchMemo::LatchMemo(DiskBufferPool *buffer_pool) : buffer_pool_(buffer_pool) {}

// LatchMemo析构函数
LatchMemo::~LatchMemo() { this->release(); } // 释放资源

RC LatchMemo::get_page(PageNum page_num, Frame *&frame) {
  frame = nullptr;

  RC rc = buffer_pool_->get_this_page(page_num, &frame); // 获取指定页
  if (rc != RC::SUCCESS) {
    return rc; // 返回错误代码
  }

  items_.emplace_back(LatchMemoType::PIN, frame); // 将帧添加到记录中
  return RC::SUCCESS;
}

RC LatchMemo::allocate_page(Frame *&frame) {
  frame = nullptr;

  RC rc = buffer_pool_->allocate_page(&frame); // 分配新页
  if (rc == RC::SUCCESS) {
    items_.emplace_back(LatchMemoType::PIN, frame); // 记录分配的帧
    ASSERT(frame->pin_count() == 1, "allocate a new frame. frame=%s", frame->to_string().c_str());
  }

  return rc;
}

// 处理被处理的页面
void LatchMemo::dispose_page(PageNum page_num) {
  disposed_pages_.emplace_back(page_num); // 记录已处理页面
}

// 锁定帧
void LatchMemo::latch(Frame *frame, LatchMemoType type) {
  switch (type) {
    case LatchMemoType::EXCLUSIVE: {
      frame->write_latch(); // 获取排他锁
    } break;
    case LatchMemoType::SHARED: {
      frame->read_latch(); // 获取共享锁
    } break;
    default: {
      ASSERT(false, "invalid latch type: %d", static_cast<int>(type)); // 检查锁类型
    }
  }

  items_.emplace_back(type, frame); // 记录锁定的帧
}

// 获取排他锁的简化函数
void LatchMemo::xlatch(Frame *frame) { this->latch(frame, LatchMemoType::EXCLUSIVE); }

// 获取共享锁的简化函数
void LatchMemo::slatch(Frame *frame) { this->latch(frame, LatchMemoType::SHARED); }

// 尝试获取共享锁
bool LatchMemo::try_slatch(Frame *frame) {
  bool ret = frame->try_read_latch(); // 尝试获取共享锁
  if (ret) {
    items_.emplace_back(LatchMemoType::SHARED, frame); // 记录锁定的帧
  }
  return ret;
}

// 锁定共享锁
void LatchMemo::xlatch(common::SharedMutex *lock) {
  lock->lock(); // 锁定
  items_.emplace_back(LatchMemoType::EXCLUSIVE, lock); // 记录锁定状态
  LOG_DEBUG("lock root success");
}

// 锁定共享锁
void LatchMemo::slatch(common::SharedMutex *lock) {
  lock->lock_shared(); // 锁定共享
  items_.emplace_back(LatchMemoType::SHARED, lock); // 记录锁定状态
}

// 释放记录中的项
void LatchMemo::release_item(LatchMemoItem &item) {
  switch (item.type) {
    case LatchMemoType::EXCLUSIVE: {
      if (item.frame != nullptr) {
        item.frame->write_unlatch(); // 释放排他锁
      } else {
        LOG_DEBUG("release root lock");
        item.lock->unlock(); // 释放共享锁
      }
    } break;
    case LatchMemoType::SHARED: {
      if (item.frame != nullptr) {
        item.frame->read_unlatch(); // 释放共享锁
      } else {
        item.lock->unlock_shared(); // 释放共享锁
      }
    } break;
    case LatchMemoType::PIN: {
      buffer_pool_->unpin_page(item.frame); // 解除页面的固定
    } break;

    default: {
      ASSERT(false, "invalid latch type: %d", static_cast<int>(item.type)); // 检查锁类型
    }
  }
}

// 释放所有资源
void LatchMemo::release() {
  int point = static_cast<int>(items_.size()); // 获取当前记录大小
  release_to(point); // 释放所有项

  for (PageNum page_num : disposed_pages_) {
    buffer_pool_->dispose_page(page_num); // 处理已释放页面
  }
  disposed_pages_.clear(); // 清空处理列表
}

// 释放到指定点
void LatchMemo::release_to(int point) {
  ASSERT(point >= 0 && point <= static_cast<int>(items_.size()), 
         "invalid memo point. point=%d, items size=%d",
         point, static_cast<int>(items_.size())); // 检查有效性

  auto iter = items_.begin();
  for (int i = point - 1; i >= 0; i--, ++iter) {
    LatchMemoItem &item = items_[i];
    release_item(item); // 释放项
  }
  items_.erase(items_.begin(), iter); // 清除已释放的项
}

