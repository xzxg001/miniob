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
// Created by Wangyunlai on 2024/02/02.
// 

#include "storage/record/record_log.h"
#include "common/log/log.h"
#include "common/lang/sstream.h"
#include "common/lang/defer.h"
#include "storage/clog/log_handler.h"
#include "storage/record/record.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/clog/log_entry.h"
#include "storage/clog/vacuous_log_handler.h"
#include "storage/record/record_manager.h"
#include "storage/buffer/frame.h"
#include "storage/record/record_log.h"

using namespace common;

// 类 RecordOperation 的实现

string RecordOperation::to_string() const
{
  string ret = std::to_string(type_id()) + ":"; // 将操作类型的 ID 转换为字符串
  switch (type_) {
    case Type::INIT_PAGE: return ret + "INIT_PAGE"; // 初始化页面操作
    case Type::INSERT: return ret + "INSERT"; // 插入操作
    case Type::DELETE: return ret + "DELETE"; // 删除操作
    case Type::UPDATE: return ret + "UPDATE"; // 更新操作
    default: return ret + "UNKNOWN"; // 未知操作
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 结构体 RecordLogHeader 的实现

const int32_t RecordLogHeader::SIZE = sizeof(RecordLogHeader); // 定义日志头的大小

string RecordLogHeader::to_string() const
{
  stringstream ss;
  ss << "buffer_pool_id:" << buffer_pool_id << ", operation_type:" << RecordOperation(operation_type).to_string()
     << ", page_num:" << page_num;

  switch (RecordOperation(operation_type).type()) {
    case RecordOperation::Type::INIT_PAGE: {
      ss << ", record_size:" << record_size; // 记录大小
    } break;
    case RecordOperation::Type::INSERT:
    case RecordOperation::Type::DELETE:
    case RecordOperation::Type::UPDATE: {
      ss << ", slot_num:" << slot_num; // 插槽编号
    } break;
    default: {
      ss << ", unknown operation type"; // 未知操作类型
    } break;
  }

  return ss.str(); // 返回日志头的字符串表示
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 类 RecordLogHandler 的实现

RC RecordLogHandler::init(
    LogHandler &log_handler, int32_t buffer_pool_id, int32_t record_size, StorageFormat storage_format)
{
  RC rc = RC::SUCCESS; // 初始化返回状态

  log_handler_    = &log_handler; // 设置日志处理器
  buffer_pool_id_ = buffer_pool_id; // 设置缓冲池 ID
  record_size_    = record_size; // 设置记录大小
  storage_format_ = storage_format; // 设置存储格式

  return rc; // 返回初始化结果
}

// data 是页面中的列索引
RC RecordLogHandler::init_new_page(Frame *frame, PageNum page_num, span<const char> data)
{
  const int log_payload_size = RecordLogHeader::SIZE + data.size(); // 计算日志负载大小
  vector<char> log_payload(log_payload_size); // 创建日志负载向量
  RecordLogHeader *header = reinterpret_cast<RecordLogHeader *>(log_payload.data()); // 获取日志头指针
  header->buffer_pool_id  = buffer_pool_id_; // 设置缓冲池 ID
  header->operation_type  = RecordOperation(RecordOperation::Type::INIT_PAGE).type_id(); // 设置操作类型
  header->page_num        = page_num; // 设置页面编号
  header->record_size     = record_size_; // 设置记录大小
  header->storage_format  = static_cast<int>(storage_format_); // 设置存储格式
  header->column_num      = data.size() / sizeof(int); // 计算列数
  if (data.size() > 0) {
    memcpy(log_payload.data() + RecordLogHeader::SIZE, data.data(), data.size()); // 复制数据
  }

  LSN lsn = 0; // 初始化日志序列号
  RC  rc  = log_handler_->append(lsn, LogModule::Id::RECORD_MANAGER, std::move(log_payload)); // 追加日志
  if (OB_SUCC(rc) && lsn > 0) {
    frame->set_lsn(lsn); // 设置帧的 LSN
  }
  return rc; // 返回操作结果
}

RC RecordLogHandler::insert_record(Frame *frame, const RID &rid, const char *record)
{
  const int log_payload_size = RecordLogHeader::SIZE + record_size_; // 计算日志负载大小
  vector<char> log_payload(log_payload_size); // 创建日志负载向量
  RecordLogHeader *header = reinterpret_cast<RecordLogHeader *>(log_payload.data()); // 获取日志头指针
  header->buffer_pool_id  = buffer_pool_id_; // 设置缓冲池 ID
  header->operation_type  = RecordOperation(RecordOperation::Type::INSERT).type_id(); // 设置操作类型
  header->page_num        = rid.page_num; // 设置页面编号
  header->slot_num        = rid.slot_num; // 设置插槽编号
  header->storage_format  = static_cast<int>(storage_format_); // 设置存储格式
  memcpy(log_payload.data() + RecordLogHeader::SIZE, record, record_size_); // 复制记录数据

  LSN lsn = 0; // 初始化日志序列号
  RC  rc  = log_handler_->append(lsn, LogModule::Id::RECORD_MANAGER, std::move(log_payload)); // 追加日志
  if (OB_SUCC(rc) && lsn > 0) {
    frame->set_lsn(lsn); // 设置帧的 LSN
  }
  return rc; // 返回操作结果
}

RC RecordLogHandler::update_record(Frame *frame, const RID &rid, const char *record)
{
  const int log_payload_size = RecordLogHeader::SIZE + record_size_; // 计算日志负载大小
  vector<char> log_payload(log_payload_size); // 创建日志负载向量
  RecordLogHeader *header = reinterpret_cast<RecordLogHeader *>(log_payload.data()); // 获取日志头指针
  header->buffer_pool_id  = buffer_pool_id_; // 设置缓冲池 ID
  header->operation_type  = RecordOperation(RecordOperation::Type::UPDATE).type_id(); // 设置操作类型
  header->page_num        = rid.page_num; // 设置页面编号
  header->slot_num        = rid.slot_num; // 设置插槽编号
  header->storage_format  = static_cast<int>(storage_format_); // 设置存储格式
  memcpy(log_payload.data() + RecordLogHeader::SIZE, record, record_size_); // 复制记录数据

  LSN lsn = 0; // 初始化日志序列号
  RC  rc  = log_handler_->append(lsn, LogModule::Id::RECORD_MANAGER, std::move(log_payload)); // 追加日志
  if (OB_SUCC(rc) && lsn > 0) {
    frame->set_lsn(lsn); // 设置帧的 LSN
  }
  return rc; // 返回操作结果
}

RC RecordLogHandler::delete_record(Frame *frame, const RID &rid)
{
  RecordLogHeader header; // 创建日志头
  header.buffer_pool_id = buffer_pool_id_; // 设置缓冲池 ID
  header.operation_type = RecordOperation(RecordOperation::Type::DELETE).type_id(); // 设置操作类型
  header.page_num       = rid.page_num; // 设置页面编号
  header.slot_num       = rid.slot_num; // 设置插槽编号
  header.storage_format = static_cast<int>(storage_format_); // 设置存储格式

  LSN lsn = 0; // 初始化日志序列号
  RC  rc  = log_handler_->append(lsn,
      LogModule::Id::RECORD_MANAGER,
      span<const char>(reinterpret_cast<const char *>(&header), RecordLogHeader::SIZE)); // 追加日志
  if (OB_SUCC(rc) && lsn > 0) {
    frame->set_lsn(lsn); // 设置帧的 LSN
  }
  return rc; // 返回操作结果
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 类 RecordLogReplayer 的实现

RecordLogReplayer::RecordLogReplayer(BufferPoolManager &bpm) : bpm_(bpm) {} // 构造函数

RC RecordLogReplayer::replay(const LogEntry &entry)
{
  LOG_TRACE("replaying record manager log: %s", entry.to_string().c_str()); // 记录重放日志

  if (entry.module().id() != LogModule::Id::RECORD_MANAGER) {
    return RC::INVALID_ARGUMENT; // 检查模块 ID
  }

  if (entry.payload_size() < RecordLogHeader::SIZE) {
    LOG_WARN("invalid log entry. payload size: %d is less than record log header size %d", 
             entry.payload_size(), RecordLogHeader::SIZE); // 检查有效负载大小
    return RC::INVALID_ARGUMENT;
  }

  auto log_header = reinterpret_cast<const RecordLogHeader *>(entry.data()); // 获取日志头

  DiskBufferPool *buffer_pool = nullptr; // 定义缓冲池指针
  Frame          *frame       = nullptr; // 定义帧指针
  RC              rc          = bpm_.get_buffer_pool(log_header->buffer_pool_id, buffer_pool); // 获取缓冲池
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to get buffer pool. buffer pool id=%d, rc=%s", log_header->buffer_pool_id, strrc(rc)); // 错误处理
    return rc;
  }

  rc = buffer_pool->get_this_page(log_header->page_num, &frame); // 获取当前页面
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to get this page. page num=%d, rc=%s", log_header->page_num, strrc(rc)); // 错误处理
    return rc;
  }

  DEFER(buffer_pool->unpin_page(frame)); // 确保在函数结束时释放页面

  const LSN frame_lsn = frame->lsn(); // 获取帧的 LSN

  if (frame_lsn >= entry.lsn()) {
    LOG_TRACE("page %d has been initialized, skip replaying record log. frame lsn %d, log lsn %d", 
              log_header->page_num, frame_lsn, entry.lsn()); // 如果帧已初始化，跳过重放
    return RC::SUCCESS;
  }

  switch (RecordOperation(log_header->operation_type).type()) {
    case RecordOperation::Type::INIT_PAGE: {
      rc = replay_init_page(*buffer_pool, *log_header); // 重放初始化页面操作
    } break;
    case RecordOperation::Type::INSERT: {
      rc = replay_insert(*buffer_pool, *log_header); // 重放插入操作
    } break;
    case RecordOperation::Type::DELETE: {
      rc = replay_delete(*buffer_pool, *log_header); // 重放删除操作
    } break;
    case RecordOperation::Type::UPDATE: {
      rc = replay_update(*buffer_pool, *log_header); // 重放更新操作
    } break;
    default: {
      LOG_WARN("unknown record operation type: %d", log_header->operation_type); // 未知操作类型处理
      return RC::INVALID_ARGUMENT;
    }
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("fail to replay record log. log header: %s, rc=%s", log_header->to_string().c_str(), strrc(rc)); // 错误处理
    return rc;
  }

  frame->set_lsn(entry.lsn()); // 设置帧的 LSN
  return RC::SUCCESS; // 返回成功状态
}

RC RecordLogReplayer::replay_init_page(DiskBufferPool &buffer_pool, const RecordLogHeader &log_header)
{
  VacuousLogHandler             vacuous_log_handler; // 创建一个无效日志处理器
  unique_ptr<RecordPageHandler> record_page_handler(
      RecordPageHandler::create(StorageFormat(log_header.storage_format))); // 创建记录页面处理器

  RC rc = record_page_handler->init_empty_page(buffer_pool,
      vacuous_log_handler,
      log_header.page_num,
      log_header.record_size,
      log_header.column_num,
      log_header.data); // 初始化空页面
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to init record page handler. page num=%d, rc=%s", log_header.page_num, strrc(rc)); // 错误处理
    return rc;
  }

  return rc; // 返回操作结果
}

RC RecordLogReplayer::replay_insert(DiskBufferPool &buffer_pool, const RecordLogHeader &log_header)
{
  VacuousLogHandler             vacuous_log_handler; // 创建一个无效日志处理器
  unique_ptr<RecordPageHandler> record_page_handler(
      RecordPageHandler::create(StorageFormat(log_header.storage_format))); // 创建记录页面处理器

  RC rc = record_page_handler->init(buffer_pool, vacuous_log_handler, log_header.page_num, ReadWriteMode::READ_WRITE); // 初始化页面
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to init record page handler. page num=%d, rc=%s", log_header.page_num, strrc(rc)); // 错误处理
    return rc;
  }

  const char *record = log_header.data; // 获取记录数据
  RID         rid(log_header.page_num, log_header.slot_num); // 创建记录 ID
  rc = record_page_handler->insert_record(record, &rid); // 插入记录
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to recover insert record. page num=%d, slot num=%d, rc=%s", 
             log_header.page_num, log_header.slot_num, strrc(rc)); // 错误处理
    return rc;
  }

  return rc; // 返回操作结果
}

RC RecordLogReplayer::replay_delete(DiskBufferPool &buffer_pool, const RecordLogHeader &log_header)
{
  VacuousLogHandler             vacuous_log_handler; // 创建一个无效日志处理器
  unique_ptr<RecordPageHandler> record_page_handler(
      RecordPageHandler::create(StorageFormat(log_header.storage_format))); // 创建记录页面处理器

  RC rc = record_page_handler->init(buffer_pool, vacuous_log_handler, log_header.page_num, ReadWriteMode::READ_WRITE); // 初始化页面
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to init record page handler. page num=%d, rc=%s", log_header.page_num, strrc(rc)); // 错误处理
    return rc;
  }

  RID rid(log_header.page_num, log_header.slot_num); // 创建记录 ID
  rc = record_page_handler->delete_record(&rid); // 删除记录
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to recover delete record. page num=%d, slot num=%d, rc=%s", 
             log_header.page_num, log_header.slot_num, strrc(rc)); // 错误处理
    return rc;
  }

  return rc; // 返回操作结果
}

RC RecordLogReplayer::replay_update(DiskBufferPool &buffer_pool, const RecordLogHeader &header)
{
  VacuousLogHandler             vacuous_log_handler; // 创建一个无效日志处理器
  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(StorageFormat(header.storage_format))); // 创建记录页面处理器

  RC rc = record_page_handler->init(buffer_pool, vacuous_log_handler, header.page_num, ReadWriteMode::READ_WRITE); // 初始化页面
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to init record page handler. page num=%d, rc=%s", header.page_num, strrc(rc)); // 错误处理
    return rc;
  }

  RID rid(header.page_num, header.slot_num); // 创建记录 ID
  rc = record_page_handler->update_record(rid, header.data); // 更新记录
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to recover update record. page num=%d, slot num=%d, rc=%s", 
             header.page_num, header.slot_num, strrc(rc)); // 错误处理
    return rc;
  }

  return rc; // 返回操作结果
}
