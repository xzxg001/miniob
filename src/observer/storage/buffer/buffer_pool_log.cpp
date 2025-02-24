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
// Created by wangyunlai on 2022/02/01
//

#include "storage/buffer/buffer_pool_log.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/clog/log_handler.h"
#include "storage/clog/log_entry.h"

/**
 * 将BufferPoolLogEntry对象的内容转换为字符串表示
 * 
 * 此函数主要用于调试和日志记录目的，通过拼接成员变量buffer_pool_id、page_num
 * 和operation_type的字符串表示形式，来构造一个描述BufferPoolLogEntry对象的字符串
 * 
 * @return 返回一个包含对象关键信息的字符串
 */
string BufferPoolLogEntry::to_string() const
{
  // 将buffer_pool_id转换为字符串并拼接
  return string("buffer_pool_id=") + std::to_string(buffer_pool_id) +
         // 将page_num转换为字符串并拼接
         ", page_num=" + std::to_string(page_num) +
         // 将operation_type转换为字符串并拼接，这里通过BufferPoolOperation类型来获取操作类型的字符串表示
         ", operation_type=" + BufferPoolOperation(operation_type).to_string();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// BufferPoolLogHandler类的构造函数
// 该构造函数初始化了BufferPoolLogHandler类的实例，使其与一个磁盘缓冲池和一个日志处理器关联
// 参数buffer_pool是BufferPoolLogHandler将要关联的磁盘缓冲池，用于处理缓冲区的管理
// 参数log_handler是BufferPoolLogHandler将要关联的日志处理器，用于实际的日志处理工作
BufferPoolLogHandler::BufferPoolLogHandler(DiskBufferPool &buffer_pool, LogHandler &log_handler)
    : buffer_pool_(buffer_pool), log_handler_(log_handler)
{}

/**
 * 分配页面并记录操作日志
 * 
 * @param page_num 页面编号，用于指定需要分配的页面
 * @param lsn 日志序列号，用于记录操作的日志位置
 * 
 * @return 返回操作的结果，表示页面分配是否成功
 * 
 * 此函数通过调用append_log方法来记录一次ALLOCATE操作，以实现页面分配
 * 它是BufferPoolLogHandler类的一部分，负责处理缓冲池中的页面分配日志记录
 */
RC BufferPoolLogHandler::allocate_page(PageNum page_num, LSN &lsn)
{
  return append_log(BufferPoolOperation::Type::ALLOCATE, page_num, lsn);
}

/**
 * Deallocates a specified page from the buffer pool and logs the deallocation event.
 * 
 * This function appends a deallocation log entry to the log sequence, recording the deallocation operation of a page.
 * It is a critical part of buffer pool management, supporting the recycling and management of page resources.
 * 
 * @param page_num The page number of the page to be deallocated.
 * @param lsn The Log Sequence Number of the log entry, used to mark the position of the log.
 * 
 * @return Returns the result of the deallocation operation, indicating whether the operation was successful.
 */
RC BufferPoolLogHandler::deallocate_page(PageNum page_num, LSN &lsn)
{
  // Appends a deallocation log entry, recording the details of the deallocation operation.
  return append_log(BufferPoolOperation::Type::DEALLOCATE, page_num, lsn);
}

/**
 * 将指定的页面刷新到磁盘。
 * 
 * 此函数的目的是将缓冲池中修改过的页面同步到磁盘上，以确保数据的一致性和持久性。
 * 它实际上并不执行物理上的数据写入操作，而是通过日志管理器等待指定的LSN（日志序列号）
 * 来确保所有在此之前的日志记录都已经写入且持久化，从而间接确保页面内容的持久化。
 * 
 * @param page 待刷新的页面引用。这个页面包含了需要持久化的数据以及它的LSN。
 * @return RC 表示操作的返回状态。通过这个返回值，调用者可以知道刷新操作是否成功。
 */
RC BufferPoolLogHandler::flush_page(Page &page)
{
  // 等待日志管理器处理到指定的LSN，以确保所有日志记录都已持久化。
  return log_handler_.wait_lsn(page.lsn);
}

/**
 * 向日志中追加缓冲池操作记录
 * 
 * @param type 缓冲池操作类型，表示执行的具体操作
 * @param page_num 操作涉及的页面编号
 * @param lsn 日志序列号，用于标识日志条目的位置
 * 
 * @return 返回操作结果，成功或失败
 * 
 * 此函数用于将缓冲池中的操作记录到日志中，以便于后续的恢复和审计操作
 * 它首先创建一个BufferPoolLogEntry对象，填充必要的信息，包括缓冲池ID、页面编号和操作类型
 * 然后调用log_handler_的append方法将日志条目追加到日志中
 */
RC BufferPoolLogHandler::append_log(BufferPoolOperation::Type type, PageNum page_num, LSN &lsn)
{
  // 创建日志条目对象并填充必要信息
  BufferPoolLogEntry log;
  log.buffer_pool_id = buffer_pool_.id();
  log.page_num = page_num;
  log.operation_type = BufferPoolOperation(type).type_id();

  // 调用日志处理器的append方法，将日志条目追加到日志中
  return log_handler_.append(lsn, LogModule::Id::BUFFER_POOL, span<const char>(reinterpret_cast<const char *>(&log), sizeof(log)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BufferPoolLogReplayer
// 构造函数：初始化日志重放器并关联到缓冲池管理器
// 参数 bp_manager: 缓冲池管理器实例的引用，用于日志重放器与之关联
BufferPoolLogReplayer::BufferPoolLogReplayer(BufferPoolManager &bp_manager) : bp_manager_(bp_manager)
{}

/**
 * 重放缓冲池日志条目
 * 
 * 此函数负责解析和执行与缓冲池相关的日志条目，以确保数据的一致性和完整性
 * 它首先验证日志条目的有效性，然后根据日志条目中的操作类型
 * 执行相应的缓冲池操作（如分配或释放页面）
 * 
 * @param entry 日志条目对象，包含缓冲池操作的信息
 * @return RC 返回状态码，表示操作的成功或失败
 */
RC BufferPoolLogReplayer::replay(const LogEntry &entry)
{
  // 验证日志条目的payload大小是否符合预期
  if (entry.payload_size() != sizeof(BufferPoolLogEntry)) {
    LOG_ERROR("invalid buffer pool log entry. payload size=%d, expected=%d, entry=%s",
              entry.payload_size(), sizeof(BufferPoolLogEntry), entry.to_string().c_str());
    return RC::INVALID_ARGUMENT;
  }

  // 解析日志条目中的缓冲池日志信息
  auto log = reinterpret_cast<const BufferPoolLogEntry *>(entry.data());

  LOG_TRACE("replay buffer pool log. entry=%s, log=%s", entry.to_string().c_str(), log->to_string().c_str());
  
  // 获取缓冲池ID，并根据ID获取对应的缓冲池实例
  int32_t buffer_pool_id = log->buffer_pool_id;
  DiskBufferPool *buffer_pool = nullptr;
  RC rc = bp_manager_.get_buffer_pool(buffer_pool_id, buffer_pool);
  if (OB_FAIL(rc) || buffer_pool == nullptr) {
    LOG_ERROR("failed to get buffer pool. rc=%s, buffer pool=%p, log=%s, %s", 
              strrc(rc), buffer_pool, entry.to_string().c_str(), log->to_string().c_str());
    return rc;
  }

  // 根据日志中的操作类型，执行相应的缓冲池操作
  BufferPoolOperation operation(log->operation_type);
  switch (operation.type())
  {
    case BufferPoolOperation::Type::ALLOCATE:
      // 重放分配页面的操作
      return buffer_pool->redo_allocate_page(entry.lsn(), log->page_num);
    case BufferPoolOperation::Type::DEALLOCATE:
      // 重放释放页面的操作
      return buffer_pool->redo_deallocate_page(entry.lsn(), log->page_num);
    default:
      // 如果操作类型未知，记录错误并返回内部错误状态码
      LOG_ERROR("unknown buffer pool operation. operation=%s", operation.to_string().c_str());
      return RC::INTERNAL;
  }
  // 操作成功完成，返回成功状态码
  return RC::SUCCESS;
}