/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/4/13.
//
#include "storage/record/record_manager.h"
#include "common/log/log.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"
#include "storage/clog/log_handler.h"

using namespace common;

static constexpr int PAGE_HEADER_SIZE = (sizeof(PageHeader));

// 创建记录页面处理器，根据存储格式选择具体实现
RecordPageHandler *RecordPageHandler::create(StorageFormat format) {
  if (format == StorageFormat::ROW_FORMAT) {
    return new RowRecordPageHandler(); // 返回行格式处理器
  } else {
    return new PaxRecordPageHandler(); // 返回列格式处理器
  }
}

/**
 * @brief 8字节对齐
 * 注: ceiling(a / b) = floor((a + b - 1) / b)
 *
 * @param size 待对齐的字节数
 */
int align8(int size) { return (size + 7) & ~7; }

/**
 * @brief 计算指定大小的页面，可以容纳多少个记录
 *
 * @param page_size   页面的大小
 * @param record_size 记录的大小
 * @param fixed_size  除 PAGE_HEADER 外，页面中其余固定长度占用，目前为PAX存储格式中的
 *                    列偏移索引大小（column index）。
 */
int page_record_capacity(int page_size, int record_size, int fixed_size) {
  // 计算可以容纳的记录数量
  return (int)((page_size - PAGE_HEADER_SIZE - fixed_size - 1) / (record_size + 0.125));
}

/**
 * @brief bitmap 记录了某个位置是否有有效的记录数据，这里给定记录个数时需要多少字节来存放bitmap数据
 * 注: ceiling(a / b) = floor((a + b - 1) / b)
 *
 * @param record_capacity 想要存放多少记录
 */
int page_bitmap_size(int record_capacity) { return (record_capacity + 7) / 8; }

// 将页面头信息转为字符串
string PageHeader::to_string() const {
  stringstream ss;
  ss << "record_num:" << record_num << ",column_num:" << column_num << ",record_real_size:" << record_real_size
     << ",record_size:" << record_size << ",record_capacity:" << record_capacity << ",data_offset:" << data_offset;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
RecordPageIterator::RecordPageIterator() {}
RecordPageIterator::~RecordPageIterator() {}

// 初始化记录页面迭代器
void RecordPageIterator::init(RecordPageHandler *record_page_handler, SlotNum start_slot_num /*=0*/) {
  record_page_handler_ = record_page_handler;
  page_num_            = record_page_handler->get_page_num();
  bitmap_.init(record_page_handler->bitmap_, record_page_handler->page_header_->record_capacity);
  next_slot_num_ = bitmap_.next_setted_bit(start_slot_num); // 获取下一个已设置的槽位
}

// 检查是否还有下一个记录
bool RecordPageIterator::has_next() { return -1 != next_slot_num_; }

// 获取下一个记录
RC RecordPageIterator::next(Record &record) {
  record_page_handler_->get_record(RID(page_num_, next_slot_num_), record); // 获取记录

  if (next_slot_num_ >= 0) {
    next_slot_num_ = bitmap_.next_setted_bit(next_slot_num_ + 1); // 更新下一个槽位
  }
  return record.rid().slot_num != -1 ? RC::SUCCESS : RC::RECORD_EOF; // 成功或到达结束
}

////////////////////////////////////////////////////////////////////////////////

RecordPageHandler::~RecordPageHandler() { cleanup(); } // 析构函数，清理资源

// 初始化记录页面
RC RecordPageHandler::init(DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num, ReadWriteMode mode) {
  if (disk_buffer_pool_ != nullptr) {
    if (frame_->page_num() == page_num) {
      LOG_WARN("Disk buffer pool has been opened for page_num %d.", page_num);
      return RC::RECORD_OPENNED; // 页面已打开
    } else {
      cleanup(); // 清理当前页面
    }
  }

  RC ret = RC::SUCCESS;
  if ((ret = buffer_pool.get_this_page(page_num, &frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to get page handle from disk buffer pool. ret=%d:%s", ret, strrc(ret));
    return ret; // 获取页面句柄失败
  }

  char *data = frame_->data();

  // 根据读写模式加锁
  if (mode == ReadWriteMode::READ_ONLY) {
    frame_->read_latch(); // 读锁
  } else {
    frame_->write_latch(); // 写锁
  }
  disk_buffer_pool_ = &buffer_pool;

  rw_mode_     = mode;
  page_header_ = (PageHeader *)(data); // 设置页面头
  bitmap_      = data + PAGE_HEADER_SIZE; // 设置位图

  (void)log_handler_.init(log_handler, buffer_pool.id(), page_header_->record_real_size, storage_format_); // 初始化日志处理器

  LOG_TRACE("Successfully init page_num %d.", page_num);
  return ret; // 返回初始化结果
}

// 恢复页面初始化
RC RecordPageHandler::recover_init(DiskBufferPool &buffer_pool, PageNum page_num) {
  if (disk_buffer_pool_ != nullptr) {
    LOG_WARN("Disk buffer pool has been opened for page_num %d.", page_num);
    return RC::RECORD_OPENNED; // 页面已打开
  }

  RC ret = RC::SUCCESS;
  if ((ret = buffer_pool.get_this_page(page_num, &frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to get page handle from disk buffer pool. ret=%d:%s", ret, strrc(ret));
    return ret; // 获取页面句柄失败
  }

  char *data = frame_->data();

  frame_->write_latch(); // 写锁
  disk_buffer_pool_ = &buffer_pool;
  rw_mode_          = ReadWriteMode::READ_WRITE; // 设置读写模式
  page_header_      = (PageHeader *)(data);
  bitmap_           = data + PAGE_HEADER_SIZE;

  buffer_pool.recover_page(page_num); // 恢复页面

  LOG_TRACE("Successfully init page_num %d.", page_num);
  return ret; // 返回初始化结果
}

// 初始化空页面
RC RecordPageHandler::init_empty_page(
    DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num, int record_size, TableMeta *table_meta) {
  RC rc = init(buffer_pool, log_handler, page_num, ReadWriteMode::READ_WRITE); // 初始化页面
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page page_num:record_size %d:%d. rc=%s", page_num, record_size, strrc(rc));
    return rc; // 返回初始化失败
  }

  (void)log_handler_.init(log_handler, buffer_pool.id(), record_size, storage_format_); // 初始化日志处理器

  int column_num = 0;
  // 仅在PAX格式中需要列索引
  if (table_meta != nullptr && storage_format_ == StorageFormat::PAX_FORMAT) {
    column_num = table_meta->field_num(); // 获取列数
  }
  
  // 初始化页面头
  page_header_->record_num       = 0;
  page_header_->column_num       = column_num;
  page_header_->record_real_size = record_size;
  page_header_->record_size      = align8(record_size); // 对齐记录大小
  page_header_->record_capacity  = page_record_capacity(
      BP_PAGE_DATA_SIZE, page_header_->record_size, column_num * sizeof(int) /* 其他固定大小*/);
  page_header_->col_idx_offset = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity));
  page_header_->data_offset    = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity)) +
                              column_num * sizeof(int) /* 列索引*/;
  this->fix_record_capacity(); // 修正记录容量
  ASSERT(page_header_->data_offset + page_header_->record_capacity * page_header_->record_size 
              <= BP_PAGE_DATA_SIZE, 
         "Record overflow the page size"); // 检查记录是否超出页面大小

  bitmap_ = frame_->data() + PAGE_HEADER_SIZE;
  memset(bitmap_, 0, page_bitmap_size(page_header_->record_capacity)); // 清空位图
  
  // 初始化列索引
  int *column_index = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  for (int i = 0; i < column_num; ++i) {
    ASSERT(i == table_meta->field(i)->field_id(), "i should be the col_id of fields[i]");
    if (i == 0) {
      column_index[i] = table_meta->field(i)->len() * page_header_->record_capacity;
    } else {
      column_index[i] = table_meta->field(i)->len() * page_header_->record_capacity + column_index[i - 1];
    }
  }

  rc = log_handler_.init_new_page(frame_, page_num, span((const char *)column_index, column_num * sizeof(int))); // 初始化新页面
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page: write log failed. page_num:record_size %d:%d. rc=%s", 
              page_num, record_size, strrc(rc));
    return rc; // 返回日志写入失败
  }

  return RC::SUCCESS; // 返回成功
}

// 初始化空页面（带列索引数据）
RC RecordPageHandler::init_empty_page(DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num,
    int record_size, int column_num, const char *col_idx_data) {
  RC rc = init(buffer_pool, log_handler, page_num, ReadWriteMode::READ_WRITE); // 初始化页面
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page page_num:record_size %d:%d. rc=%s", page_num, record_size, strrc(rc));
    return rc; // 返回初始化失败
  }

  (void)log_handler_.init(log_handler, buffer_pool.id(), record_size, storage_format_); // 初始化日志处理器

  // 初始化页面头
  page_header_->record_num       = 0;
  page_header_->column_num       = column_num;
  page_header_->record_real_size = record_size;
  page_header_->record_size      = align8(record_size); // 对齐记录大小
  page_header_->record_capacity =
      page_record_capacity(BP_PAGE_DATA_SIZE, page_header_->record_size, page_header_->column_num * sizeof(int));
  page_header_->col_idx_offset = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity));
  page_header_->data_offset    = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity)) +
                              column_num * sizeof(int) /* 列索引*/;
  this->fix_record_capacity(); // 修正记录容量
  ASSERT(page_header_->data_offset + page_header_->record_capacity * page_header_->record_size 
              <= BP_PAGE_DATA_SIZE, 
         "Record overflow the page size"); // 检查记录是否超出页面大小

  bitmap_ = frame_->data() + PAGE_HEADER_SIZE;
  memset(bitmap_, 0, page_bitmap_size(page_header_->record_capacity)); // 清空位图
  
  // 复制列索引数据
  int *column_index = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  memcpy(column_index, col_idx_data, column_num * sizeof(int)); // 复制列索引数据

  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page: write log failed. page_num:record_size %d:%d. rc=%s", 
              page_num, record_size, strrc(rc));
    return rc; // 返回日志写入失败
  }

  return RC::SUCCESS; // 返回成功
}

// 清理页面
RC RecordPageHandler::cleanup() {
  if (disk_buffer_pool_ != nullptr) {
    if (rw_mode_ == ReadWriteMode::READ_ONLY) {
      frame_->read_unlatch(); // 解除读锁
    } else {
      frame_->write_unlatch(); // 解除写锁
    }
    disk_buffer_pool_->unpin_page(frame_); // 从磁盘缓冲池中释放页面
    disk_buffer_pool_ = nullptr;
  }

  return RC::SUCCESS; // 返回成功
}

// 在行记录页面中插入记录
RC RowRecordPageHandler::insert_record(const char *data, RID *rid) {
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot insert record into page while the page is readonly"); // 确保不是只读模式

  if (page_header_->record_num == page_header_->record_capacity) {
    LOG_WARN("Page is full, page_num %d:%d.", disk_buffer_pool_->file_desc(), frame_->page_num());
    return RC::RECORD_NOMEM; // 页面已满
  }

  // 找到空闲位置
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  int    index = bitmap.next_unsetted_bit(0); // 获取下一个未设置的位
  bitmap.set_bit(index); // 设置该位
  page_header_->record_num++; // 更新记录数量

  RC rc = log_handler_.insert_record(frame_, RID(get_page_num(), index), data); // 插入记录
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to insert record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
    // return rc; // 忽略错误
  }

  // 确保索引小于记录容量
  char *record_data = get_record_data(index);
  memcpy(record_data, data, page_header_->record_real_size); // 复制记录数据

  frame_->mark_dirty(); // 标记页面为脏

  if (rid) {
    rid->page_num = get_page_num();
    rid->slot_num = index; // 设置RID
  }

  // LOG_TRACE("Insert record. rid page_num=%d, slot num=%d", get_page_num(), index);
  return RC::SUCCESS; // 返回成功
}

RC RowRecordPageHandler::recover_insert_record(const char *data, const RID &rid)
{
  // 检查槽位号是否超出记录容量
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_WARN("slot_num illegal, slot_num(%d) > record_capacity(%d).", rid.slot_num, page_header_->record_capacity);
    return RC::RECORD_INVALID_RID; // 返回无效RID
  }

  // 更新位图
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid.slot_num)) { // 检查槽位是否已被使用
    bitmap.set_bit(rid.slot_num); // 设置槽位为已使用
    page_header_->record_num++; // 更新记录数量
  }

  // 恢复数据
  char *record_data = get_record_data(rid.slot_num); // 获取记录数据
  memcpy(record_data, data, page_header_->record_real_size); // 复制数据

  frame_->mark_dirty(); // 标记页面为脏

  return RC::SUCCESS; // 返回成功
}

RC RowRecordPageHandler::delete_record(const RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot delete record from page while the page is readonly"); // 确保不是只读模式

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid->slot_num)) { // 检查槽位是否已被使用
    bitmap.clear_bit(rid->slot_num); // 清除槽位
    page_header_->record_num--; // 更新记录数量
    frame_->mark_dirty(); // 标记页面为脏

    RC rc = log_handler_.delete_record(frame_, *rid); // 记录删除操作
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to delete record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // 忽略错误
    }

    return RC::SUCCESS; // 返回成功
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST; // 返回记录不存在
  }
}

RC RowRecordPageHandler::update_record(const RID &rid, const char *data)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, "cannot delete record from page while the page is readonly"); // 确保不是只读模式

  // 检查槽位号是否超出记录容量
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, frame=%s, page_header=%s",
              rid.slot_num, frame_->to_string().c_str(), page_header_->to_string().c_str());
    return RC::INVALID_ARGUMENT; // 返回无效参数
  }

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid.slot_num)) { // 检查槽位是否已被使用
    frame_->mark_dirty(); // 标记页面为脏

    char *record_data = get_record_data(rid.slot_num); // 获取记录数据
    if (record_data != data) { // 如果新数据与旧数据不同
      memcpy(record_data, data, page_header_->record_real_size); // 复制新数据
    }

    RC rc = log_handler_.update_record(frame_, rid, data); // 记录更新操作
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to update record. page_num %d:%d. rc=%s", 
                disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // 忽略错误
    }

    return RC::SUCCESS; // 返回成功
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid.slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST; // 返回记录不存在
  }
}

RC RowRecordPageHandler::get_record(const RID &rid, Record &record)
{
  // 检查槽位号是否超出记录容量
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, frame=%s, page_header=%s",
              rid.slot_num, frame_->to_string().c_str(), page_header_->to_string().c_str());
    return RC::RECORD_INVALID_RID; // 返回无效RID
  }

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid.slot_num)) { // 检查槽位是否已被使用
    LOG_ERROR("Invalid slot_num:%d, slot is empty, page_num %d.", rid.slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST; // 返回记录不存在
  }

  record.set_rid(rid); // 设置记录的RID
  record.set_data(get_record_data(rid.slot_num), page_header_->record_real_size); // 设置记录数据
  return RC::SUCCESS; // 返回成功
}

PageNum RecordPageHandler::get_page_num() const
{
  if (nullptr == page_header_) {
    return (PageNum)(-1); // 如果页面头为空，返回-1
  }
  return frame_->page_num(); // 返回页面号
}

bool RecordPageHandler::is_full() const { 
  return page_header_->record_num >= page_header_->record_capacity; // 判断页面是否已满 
}

RC PaxRecordPageHandler::insert_record(const char *data, RID *rid)
{
  // your code here
  exit(-1); // 暂未实现
}

RC PaxRecordPageHandler::delete_record(const RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot delete record from page while the page is readonly"); // 确保不是只读模式

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid->slot_num)) { // 检查槽位是否已被使用
    bitmap.clear_bit(rid->slot_num); // 清除槽位
    page_header_->record_num--; // 更新记录数量
    frame_->mark_dirty(); // 标记页面为脏

    RC rc = log_handler_.delete_record(frame_, *rid); // 记录删除操作
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to delete record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // 忽略错误
    }

    return RC::SUCCESS; // 返回成功
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST; // 返回记录不存在
  }
}

RC PaxRecordPageHandler::get_record(const RID &rid, Record &record)
{
  // your code here
  exit(-1); // 暂未实现
}

// TODO: 指定需要的列ID，目前获取所有列
RC PaxRecordPageHandler::get_chunk(Chunk &chunk)
{
  // your code here
  exit(-1); // 暂未实现
}

char *PaxRecordPageHandler::get_field_data(SlotNum slot_num, int col_id)
{
  int *col_idx = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  if (col_id == 0) {
    return frame_->data() + page_header_->data_offset + (get_field_len(col_id) * slot_num); // 获取字段数据
  } else {
    return frame_->data() + page_header_->data_offset + col_idx[col_id - 1] + (get_field_len(col_id) * slot_num); // 获取字段数据
  }
}

int PaxRecordPageHandler::get_field_len(int col_id)
{
  int *col_idx = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  if (col_id == 0) {
    return col_idx[col_id] / page_header_->record_capacity; // 计算字段长度
  } else {
    return (col_idx[col_id] - col_idx[col_id - 1]) / page_header_->record_capacity; // 计算字段长度
  }
}


////////////////////////////////////////////////////////////////////////////////

RecordFileHandler::~RecordFileHandler() { this->close(); } // 析构函数，关闭记录文件处理器

RC RecordFileHandler::init(DiskBufferPool &buffer_pool, LogHandler &log_handler, TableMeta *table_meta)
{
  // 检查记录文件处理器是否已打开
  if (disk_buffer_pool_ != nullptr) {
    LOG_ERROR("record file handler has been openned.");
    return RC::RECORD_OPENNED; // 返回已打开状态
  }

  // 初始化成员变量
  disk_buffer_pool_ = &buffer_pool;
  log_handler_      = &log_handler;
  table_meta_       = table_meta;

  RC rc = init_free_pages(); // 初始化空闲页面

  LOG_INFO("open record file handle done. rc=%s", strrc(rc)); // 记录打开日志
  return RC::SUCCESS; // 返回成功
}

void RecordFileHandler::close()
{
  // 关闭记录文件处理器
  if (disk_buffer_pool_ != nullptr) {
    free_pages_.clear(); // 清空空闲页面列表
    disk_buffer_pool_ = nullptr; // 释放指针
    log_handler_      = nullptr;
    table_meta_       = nullptr;
  }
}

RC RecordFileHandler::init_free_pages()
{
  // 遍历当前文件的所有页面，找到没有满的页面
  // 这个效率很低，会降低启动速度
  // NOTE: 由于是初始化时的动作，所以不需要加锁控制并发

  RC rc = RC::SUCCESS;

  BufferPoolIterator bp_iterator;
  bp_iterator.init(*disk_buffer_pool_, 1); // 初始化缓冲池迭代器
  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));
  PageNum current_page_num = 0;

  // 遍历缓冲池中的页面
  while (bp_iterator.has_next()) {
    current_page_num = bp_iterator.next(); // 获取当前页面号

    rc = record_page_handler->init(*disk_buffer_pool_, *log_handler_, current_page_num, ReadWriteMode::READ_ONLY);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to init record page handler. page num=%d, rc=%d:%s", current_page_num, rc, strrc(rc));
      return rc; // 初始化失败，返回错误码
    }

    // 如果页面未满，将其添加到空闲页面列表
    if (!record_page_handler->is_full()) {
      free_pages_.insert(current_page_num);
    }
    record_page_handler->cleanup(); // 清理页面处理器
  }
  LOG_INFO("record file handler init free pages done. free page num=%d, rc=%s", free_pages_.size(), strrc(rc));
  return rc; // 返回成功
}

RC RecordFileHandler::insert_record(const char *data, int record_size, RID *rid)
{
  RC ret = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));
  bool page_found = false; // 标记是否找到合适的页面
  PageNum current_page_num = 0;

  // 当前要访问free_pages对象，所以需要加锁
  lock_.lock(); // 加锁以保证线程安全

  // 找到没有填满的页面
  while (!free_pages_.empty()) {
    current_page_num = *free_pages_.begin(); // 获取第一个空闲页面

    ret = record_page_handler->init(*disk_buffer_pool_, *log_handler_, current_page_num, ReadWriteMode::READ_WRITE);
    if (OB_FAIL(ret)) {
      lock_.unlock(); // 解锁
      LOG_WARN("failed to init record page handler. page num=%d, rc=%d:%s", current_page_num, ret, strrc(ret));
      return ret; // 初始化失败，返回错误码
    }

    if (!record_page_handler->is_full()) { // 检查页面是否未满
      page_found = true; // 标记已找到合适页面
      break; // 退出循环
    }
    record_page_handler->cleanup(); // 清理页面处理器
    free_pages_.erase(free_pages_.begin()); // 移除已检查的页面
  }
  lock_.unlock(); // 解锁

  // 找不到就分配一个新的页面
  if (!page_found) {
    Frame *frame = nullptr;
    // 分配新页面
    if ((ret = disk_buffer_pool_->allocate_page(&frame)) != RC::SUCCESS) {
      LOG_ERROR("Failed to allocate page while inserting record. ret:%d", ret);
      return ret; // 分配失败，返回错误码
    }

    current_page_num = frame->page_num(); // 获取新页面号

    // 初始化空页面
    ret = record_page_handler->init_empty_page(
        *disk_buffer_pool_, *log_handler_, current_page_num, record_size, table_meta_);
    if (OB_FAIL(ret)) {
      frame->unpin(); // 释放页面
      LOG_ERROR("Failed to init empty page. ret:%d", ret);
      return ret; // 初始化失败，返回错误码
    }

    // 手动释放一个页面引用
    frame->unpin();

    // 加锁顺序不会出现死锁
    lock_.lock();
    free_pages_.insert(current_page_num); // 将新页面加入空闲页面列表
    lock_.unlock();
  }

  // 找到空闲位置，插入记录
  return record_page_handler->insert_record(data, rid);
}

RC RecordFileHandler::recover_insert_record(const char *data, int record_size, const RID &rid)
{
  RC ret = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));

  // 初始化页面处理器
  ret = record_page_handler->recover_init(*disk_buffer_pool_, rid.page_num);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init record page handler. page num=%d, rc=%s", rid.page_num, strrc(ret));
    return ret; // 初始化失败，返回错误码
  }

  // 恢复插入记录
  return record_page_handler->recover_insert_record(data, rid);
}

RC RecordFileHandler::delete_record(const RID *rid)
{
  RC rc = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));

  // 初始化页面处理器
  rc = record_page_handler->init(*disk_buffer_pool_, *log_handler_, rid->page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d. rc=%s", rid->page_num, strrc(rc));
    return rc; // 初始化失败，返回错误码
  }

  rc = record_page_handler->delete_record(rid); // 删除记录
  // 注意要清理掉资源，否则会与insert_record中的加锁顺序冲突而可能出现死锁
  // delete record的加锁逻辑是拿到页面锁，删除指定记录，然后加上和释放record manager锁
  // insert record是加上record manager锁，然后拿到指定页面锁再释放record manager锁
  record_page_handler->cleanup(); // 清理页面处理器
  if (OB_SUCC(rc)) {
    // 因为这里已经释放了页面锁，可能其他线程已将该页面填满
    lock_.lock();
    free_pages_.insert(rid->page_num); // 将页面加入空闲页面列表
    LOG_TRACE("add free page %d to free page list", rid->page_num);
    lock_.unlock();
  }
  return rc; // 返回结果
}

RC RecordFileHandler::get_record(const RID &rid, Record &record)
{
  unique_ptr<RecordPageHandler> page_handler(RecordPageHandler::create(storage_format_));

  // 初始化页面处理器
  RC rc = page_handler->init(*disk_buffer_pool_, *log_handler_, rid.page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d", rid.page_num);
    return rc; // 初始化失败，返回错误码
  }

  Record inplace_record;
  rc = page_handler->get_record(rid, inplace_record); // 获取记录
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get record from record page handle. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
    return rc; // 获取记录失败，返回错误码
  }

  record.copy_data(inplace_record.data(), inplace_record.len()); // 复制数据
  record.set_rid(rid); // 设置RID
  return rc; // 返回结果
}

RC RecordFileHandler::visit_record(const RID &rid, function<bool(Record &)> updater)
{
  unique_ptr<RecordPageHandler> page_handler(RecordPageHandler::create(storage_format_));

  // 初始化页面处理器
  RC rc = page_handler->init(*disk_buffer_pool_, *log_handler_, rid.page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d", rid.page_num);
    return rc; // 初始化失败，返回错误码
  }

  Record inplace_record;
  rc = page_handler->get_record(rid, inplace_record); // 获取记录
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get record from record page handle. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
    return rc; // 获取记录失败，返回错误码
  }
  // 在此处可以对记录进行更新
  // ...
}


  // 需要将数据复制出来再修改，否则update_record调用失败但是实际上数据却更新成功了，
  // 会导致数据库状态不正确
  Record record;
  record.copy_data(inplace_record.data(), inplace_record.len());
  record.set_rid(rid);

  bool updated = updater(record);
  if (updated) {
    rc = page_handler->update_record(rid, record.data());
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

RecordFileScanner::~RecordFileScanner() { close_scan(); } // 析构函数，关闭扫描

RC RecordFileScanner::open_scan(Table *table, DiskBufferPool &buffer_pool, Trx *trx, LogHandler &log_handler,
    ReadWriteMode mode, ConditionFilter *condition_filter)
{
  close_scan(); // 关闭之前的扫描

  // 初始化成员变量
  table_            = table;
  disk_buffer_pool_ = &buffer_pool;
  trx_              = trx;
  log_handler_      = &log_handler;
  rw_mode_          = mode;

  // 初始化缓冲池迭代器
  RC rc = bp_iterator_.init(buffer_pool, 1);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init bp iterator. rc=%d:%s", rc, strrc(rc));
    return rc; // 返回初始化失败的状态
  }

  condition_filter_ = condition_filter; // 设置条件过滤器
  // 根据表的存储格式选择记录页面处理器
  if (table == nullptr || table->table_meta().storage_format() == StorageFormat::ROW_FORMAT) {
    record_page_handler_ = new RowRecordPageHandler(); // 行格式处理器
  } else {
    record_page_handler_ = new PaxRecordPageHandler(); // 列格式处理器
  }

  return rc; // 返回成功状态
}

/**
 * @brief 从当前位置开始找到下一条有效的记录
 *
 * 如果当前页面还有记录没有访问，就遍历当前的页面。
 * 当前页面遍历完了，就遍历下一个页面，然后找到有效的记录。
 */
RC RecordFileScanner::fetch_next_record()
{
  RC rc = RC::SUCCESS;
  if (record_page_iterator_.is_valid()) {
    // 当前页面有效，尝试看是否有有效记录
    rc = fetch_next_record_in_page();
    if (rc == RC::SUCCESS || rc != RC::RECORD_EOF) {
      // 有有效记录或出现其他错误
      return rc;
    }
  }

  // 上一个页面遍历完，或者还没有开始遍历某个页面
  while (bp_iterator_.has_next()) {
    PageNum page_num = bp_iterator_.next(); // 获取下一个页面号
    record_page_handler_->cleanup(); // 清理页面处理器
    rc = record_page_handler_->init(*disk_buffer_pool_, *log_handler_, page_num, rw_mode_); // 初始化页面处理器
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to init record page handler. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc; // 初始化失败，返回错误码
    }

    record_page_iterator_.init(record_page_handler_); // 初始化记录页面迭代器
    rc = fetch_next_record_in_page(); // 尝试获取下一条记录
    if (rc == RC::SUCCESS || rc != RC::RECORD_EOF) {
      return rc; // 有有效记录或出现其他错误
    }
  }

  // 所有页面都遍历完了，没有数据了
  next_record_.rid().slot_num = -1; // 标记为无效记录
  record_page_handler_->cleanup(); // 清理页面处理器
  return RC::RECORD_EOF; // 返回记录结束状态
}

/**
 * @brief 遍历当前页面，尝试找到一条有效的记录
 */
RC RecordFileScanner::fetch_next_record_in_page()
{
  RC rc = RC::SUCCESS;
  while (record_page_iterator_.has_next()) {
    rc = record_page_iterator_.next(next_record_); // 获取下一条记录
    if (rc != RC::SUCCESS) {
      const auto page_num = record_page_handler_->get_page_num();
      LOG_TRACE("failed to get next record from page. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc; // 获取记录失败，返回错误码
    }

    // 如果有过滤条件，则应用过滤
    if (condition_filter_ != nullptr && !condition_filter_->filter(next_record_)) {
      continue; // 过滤掉不符合条件的记录
    }

    // 如果是某个事务上遍历数据，还要看看事务访问是否有冲突
    if (trx_ == nullptr) {
      return rc; // 如果没有事务，直接返回
    }

    // 让当前事务探测是否有访问冲突
    // TODO 把判断事务有效性的逻辑从Scanner中移除
    rc = trx_->visit_record(table_, next_record_, rw_mode_); // 检查记录的可见性
    if (rc == RC::RECORD_INVISIBLE) {
      // 当前记录不可见
      continue; // 继续遍历
    }
    return rc; // 返回访问结果
  }

  next_record_.rid().slot_num = -1; // 标记为无效记录
  return RC::RECORD_EOF; // 返回记录结束状态
}

RC RecordFileScanner::close_scan()
{
  if (disk_buffer_pool_ != nullptr) {
    disk_buffer_pool_ = nullptr; // 清理缓冲池
  }

  if (condition_filter_ != nullptr) {
    condition_filter_ = nullptr; // 清理条件过滤器
  }
  
  if (record_page_handler_ != nullptr) {
    record_page_handler_->cleanup(); // 清理页面处理器
    delete record_page_handler_; // 删除处理器对象
    record_page_handler_ = nullptr; // 清空指针
  }

  return RC::SUCCESS; // 返回成功状态
}

RC RecordFileScanner::next(Record &record)
{
  RC rc = fetch_next_record(); // 获取下一条记录
  if (OB_FAIL(rc)) {
    return rc; // 如果失败，返回错误码
  }

  record = next_record_; // 复制记录
  return RC::SUCCESS; // 返回成功状态
}

RC RecordFileScanner::update_current(const Record &record)
{
  if (record.rid() != next_record_.rid()) {
    return RC::INVALID_ARGUMENT; // 如果记录ID不匹配，返回无效参数
  }

  return record_page_handler_->update_record(record.rid(), record.data()); // 更新当前记录
}

ChunkFileScanner::~ChunkFileScanner() { close_scan(); } // 析构函数，关闭扫描

RC ChunkFileScanner::close_scan()
{
  if (disk_buffer_pool_ != nullptr) {
    disk_buffer_pool_ = nullptr; // 清理缓冲池
  }

  if (record_page_handler_ != nullptr) {
    record_page_handler_->cleanup(); // 清理页面处理器
    delete record_page_handler_; // 删除处理器对象
    record_page_handler_ = nullptr; // 清空指针
  }

  return RC::SUCCESS; // 返回成功状态
}

RC ChunkFileScanner::open_scan_chunk(
    Table *table, DiskBufferPool &buffer_pool, LogHandler &log_handler, ReadWriteMode mode)
{
  close_scan(); // 关闭之前的扫描

  // 初始化成员变量
  table_            = table;
  disk_buffer_pool_ = &buffer_pool;
  log_handler_      = &log_handler;
  rw_mode_          = mode;

  // 初始化缓冲池迭代器
  RC rc = bp_iterator_.init(buffer_pool, 1);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init bp iterator. rc=%d:%s", rc, strrc(rc));
    return rc; // 返回初始化失败的状态
  }

  // 根据表的存储格式选择记录页面处理器
  if (table == nullptr || table->table_meta().storage_format() == StorageFormat::ROW_FORMAT) {
    record_page_handler_ = new RowRecordPageHandler(); // 行格式处理器
  } else {
    record_page_handler_ = new PaxRecordPageHandler(); // 列格式处理器
  }

  return rc; // 返回成功状态
}

RC ChunkFileScanner::next_chunk(Chunk &chunk)
{
  RC rc = RC::SUCCESS;

  while (bp_iterator_.has_next()) {
    PageNum page_num = bp_iterator_.next(); // 获取下一个页面号
    record_page_handler_->cleanup(); // 清理页面处理器
    rc = record_page_handler_->init(*disk_buffer_pool_, *log_handler_, page_num, rw_mode_); // 初始化页面处理器
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to init record page handler. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc; // 初始化失败，返回错误码
    }
    rc = record_page_handler_->get_chunk(chunk); // 获取数据块
    if (rc == RC::SUCCESS) {
      return rc; // 返回成功状态
    } else if (rc == RC::RECORD_EOF) {
      break; // 数据块已遍历完
    } else {
      LOG_WARN("failed to get chunk from page. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc; // 获取数据块失败，返回错误码
    }
  }

  record_page_handler_->cleanup(); // 清理页面处理器
  return RC::RECORD_EOF; // 返回记录结束状态
}

