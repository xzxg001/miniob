/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// 包含管理缓冲区帧和会话处理所需的头文件
#include "storage/buffer/frame.h"
#include "session/session.h"
#include "session/thread_data.h"

// FrameId 构造函数，初始化缓冲池 ID 和页面编号
FrameId::FrameId(int buffer_pool_id, PageNum page_num) 
    : buffer_pool_id_(buffer_pool_id), page_num_(page_num) {}

// 检查两个 FrameId 对象是否相等，依据其缓冲池 ID 和页面编号
bool FrameId::equal_to(const FrameId &other) const {
    return buffer_pool_id_ == other.buffer_pool_id_ && page_num_ == other.page_num_;
}

// 重载 FrameId 的相等运算符
bool FrameId::operator==(const FrameId &other) const { 
    return this->equal_to(other); 
}

// 生成 FrameId 的哈希值，结合缓冲池 ID 和页面编号
size_t FrameId::hash() const { 
    return (static_cast<size_t>(buffer_pool_id_) << 32L) | page_num_; 
}

// 获取缓冲池 ID
int FrameId::buffer_pool_id() const { 
    return buffer_pool_id_; 
}

// 获取页面编号
PageNum FrameId::page_num() const { 
    return page_num_; 
}

// 将 FrameId 转换为字符串表示，便于调试
string FrameId::to_string() const {
    stringstream ss;
    ss << "buffer_pool_id:" << buffer_pool_id() << ",page_num:" << page_num();
    return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
// 获取默认的调试事务 ID
intptr_t get_default_debug_xid() {
#if 0
    // 下面的代码块被注释掉；它处理跨平台的线程 ID。
    ThreadData *thd = ThreadData::current();
    intptr_t xid = (thd == nullptr) ? 
        reinterpret_cast<intptr_t>(reinterpret_cast<void*>(pthread_self())) : 
        reinterpret_cast<intptr_t>(thd);
#endif
    // 获取当前会话；如果没有，则使用线程 ID
    Session *session = Session::current_session();
    if (session == nullptr) {
        return reinterpret_cast<intptr_t>(reinterpret_cast<void *>(pthread_self()));
    } else {
        return reinterpret_cast<intptr_t>(session);
    }
}

// 写锁操作
void Frame::write_latch() { 
    write_latch(get_default_debug_xid()); 
}

// 带有事务 ID 的写锁操作
void Frame::write_latch(intptr_t xid) {
    {
        scoped_lock debug_lock(debug_lock_);
        ASSERT(pin_count_.load() > 0,
            "frame lock. write lock failed while pin count is invalid. "
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

        ASSERT(read_lockers_.find(xid) == read_lockers_.end(),
            "frame lock write while holding the read lock."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());
    }

    // 获取写锁
    lock_.lock();

#ifdef DEBUG
    write_locker_ = xid; // 记录持有写锁的事务 ID
    ++write_recursive_count_; // 递归计数
    TRACE("frame write lock success."
          "this=%p, pin=%d, frameId=%s, write locker=%lx(recursive=%d), xid=%lx, lbt=%s",
          this, pin_count_.load(), frame_id_.to_string().c_str(), write_locker_, write_recursive_count_, xid, lbt());
#endif
}

// 解锁写锁
void Frame::write_unlatch() { 
    write_unlatch(get_default_debug_xid()); 
}

// 带有事务 ID 的解锁写锁
void Frame::write_unlatch(intptr_t xid) {
    // 因为当前已经加着写锁，所以不再加 debug_lock 来做校验
    debug_lock_.lock();

    ASSERT(pin_count_.load() > 0,
        "frame lock. write unlock failed while pin count is invalid."
        "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
        this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

    ASSERT(write_locker_ == xid,
        "frame unlock write while not the owner."
        "write_locker=%lx, this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
        write_locker_, this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

    TRACE("frame write unlock success. this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
          this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

    if (--write_recursive_count_ == 0) {
        write_locker_ = 0; // 释放写锁
    }
    debug_lock_.unlock();

    lock_.unlock();
}

// 读锁操作
void Frame::read_latch() { 
    read_latch(get_default_debug_xid()); 
}

// 带有事务 ID 的读锁操作
void Frame::read_latch(intptr_t xid) {
    {
        scoped_lock debug_lock(debug_lock_);
        ASSERT(pin_count_ > 0,
            "frame lock. read lock failed while pin count is invalid."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

        ASSERT(xid != write_locker_,
            "frame lock read while holding the write lock."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());
    }

    // 获取读锁
    lock_.lock_shared();

    {
#ifdef DEBUG
        scoped_lock debug_lock(debug_lock_);
        ++read_lockers_[xid]; // 记录持有读锁的事务 ID
        TRACE("frame read lock success."
              "this=%p, pin=%d, frameId=%s, xid=%lx, recursive=%d, lbt=%s",
              this, pin_count_.load(), frame_id_.to_string().c_str(), xid, read_lockers_[xid], lbt());
#endif
    }
}

// 尝试获取读锁
bool Frame::try_read_latch() {
    intptr_t xid = get_default_debug_xid();
    {
        scoped_lock debug_lock(debug_lock_);
        ASSERT(pin_count_ > 0,
            "frame try lock. read lock failed while pin count is invalid."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

        ASSERT(xid != write_locker_,
            "frame try to lock read while holding the write lock."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());
    }

    // 尝试获取读锁
    bool ret = lock_.try_lock_shared();
    if (ret) {
#ifdef DEBUG
        debug_lock_.lock();
        ++read_lockers_[xid]; // 记录持有读锁的事务 ID
        TRACE("frame read lock success."
              "this=%p, pin=%d, frameId=%s, xid=%lx, recursive=%d, lbt=%s",
              this, pin_count_.load(), frame_id_.to_string().c_str(), xid, read_lockers_[xid], lbt());
        debug_lock_.unlock();
#endif
    }

    return ret; // 返回尝试锁定的结果
}

// 解锁读锁
void Frame::read_unlatch() { 
    read_unlatch(get_default_debug_xid()); 
}

// 带有事务 ID 的解锁读锁
void Frame::read_unlatch(intptr_t xid) {
    {
        scoped_lock debug_lock(debug_lock_);
        ASSERT(pin_count_.load() > 0,
            "frame lock. read unlock failed while pin count is invalid."
            "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

#ifdef DEBUG
        auto read_lock_iter = read_lockers_.find(xid);
        int recursive_count = read_lock_iter != read_lockers_.end() ? read_lock_iter->second : 0;
        ASSERT(recursive_count > 0,
            "frame unlock while not holding read lock."
            "this=%p, pin=%d, frameId=%s, xid=%lx, recursive=%d, lbt=%s",
            this, pin_count_.load(), frame_id_.to_string().c_str(), xid, recursive_count, lbt());

        if (1 == recursive_count) {
            read_lockers_.erase(xid); // 移除事务 ID 的记录
        }
#endif
    }

    TRACE("frame read unlock success."
          "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
          this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

    lock_.unlock_shared(); // 释放读锁
}

// 针对帧的 pin 操作
void Frame::pin() {
    scoped_lock debug_lock(debug_lock_);

    [[maybe_unused]] intptr_t xid = get_default_debug_xid(); // 获取事务 ID
    [[maybe_unused]] int pin_count = ++pin_count_; // 增加 pin 计数

    TRACE("after frame pin. "
          "this=%p, write locker=%lx, read locker has xid %d? pin=%d, frameId=%s, xid=%lx, lbt=%s",
          this, write_locker_, read_lockers_.find(xid) != read_lockers_.end(), 
          pin_count, frame_id_.to_string().c_str(), xid, lbt());
}

// 解除 pin 操作
int Frame::unpin() {
    [[maybe_unused]] intptr_t xid = get_default_debug_xid(); // 获取事务 ID

    ASSERT(pin_count_.load() > 0,
        "try to unpin a frame that pin count <= 0."
        "this=%p, pin=%d, frameId=%s, xid=%lx, lbt=%s",
        this, pin_count_.load(), frame_id_.to_string().c_str(), xid, lbt());

    scoped_lock debug_lock(debug_lock_);

    int pin_count = --pin_count_; // 减少 pin 计数
    TRACE("after frame unpin. "
          "this=%p, write locker=%lx, read locker has xid? %d, pin=%d, frameId=%s, xid=%lx, lbt=%s",
          this, write_locker_, read_lockers_.find(xid) != read_lockers_.end(), 
          pin_count, frame_id_.to_string().c_str(), xid, lbt());

    // 如果 pin 计数为 0，检查是否还有持有写锁和读锁的事务
    if (0 == pin_count) {
        ASSERT(write_locker_ == 0,
               "frame unpin to 0 failed while someone hold the write lock. write locker=%lx, frameId=%s, xid=%lx",
               write_locker_, frame_id_.to_string().c_str(), xid);
        ASSERT(read_lockers_.empty(),
               "frame unpin to 0 failed while someone hold the read locks. reader num=%d, frameId=%s, xid=%lx",
               read_lockers_.size(), frame_id_.to_string().c_str(), xid);
    }
    return pin_count; // 返回当前 pin 计数
}

// 获取当前时间（以纳秒为单位）
unsigned long current_time() {
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return tp.tv_sec * 1000 * 1000 * 1000UL + tp.tv_nsec; // 将秒和纳秒转换为纳秒
}

// 更新帧的访问时间
void Frame::access() { 
    acc_time_ = current_time(); 
}

// 将 Frame 转换为字符串表示，便于调试
string Frame::to_string() const {
    stringstream ss;
    ss << "frame id:" << frame_id().to_string() << ", dirty=" << dirty() << ", pin=" << pin_count()
       << ", lsn=" << lsn(); // 包含帧 ID、脏标志、pin 计数和日志序列号
    return ss.str();
}
