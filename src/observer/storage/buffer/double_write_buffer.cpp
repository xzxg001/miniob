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
// Created by Wenbin1002 on 2024/04/16
//
#include <fcntl.h>

#include <mutex>
#include <algorithm>

#include "storage/buffer/double_write_buffer.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "common/io/io.h"
#include "common/log/log.h"
#include "common/math/crc.h"

using namespace common;

struct DoubleWritePage
{
public:
  DoubleWritePage() = default;
  DoubleWritePage(int32_t buffer_pool_id, PageNum page_num, int32_t page_index, Page &page);

public:
  DoubleWritePageKey key;
  int32_t            page_index = -1; /// 页面在double write buffer文件中的页索引
  bool               valid = true; /// 表示页面是否有效，在页面被删除时，需要同时标记磁盘上的值。
  Page               page;

  static const int32_t SIZE;
};

DoubleWritePage::DoubleWritePage(int32_t buffer_pool_id, PageNum page_num, int32_t page_index, Page &_page)
  : key{buffer_pool_id, page_num}, page_index(page_index), page(_page)
{}

const int32_t DoubleWritePage::SIZE = sizeof(DoubleWritePage);

const int32_t DoubleWriteBufferHeader::SIZE = sizeof(DoubleWriteBufferHeader);

DiskDoubleWriteBuffer::DiskDoubleWriteBuffer(BufferPoolManager &bp_manager, int max_pages /*=16*/) 
  : max_pages_(max_pages), bp_manager_(bp_manager)
{
}

DiskDoubleWriteBuffer::~DiskDoubleWriteBuffer()
{
  flush_page();
  close(file_desc_);
}

/**
 * 打开用于双重写入缓冲的文件。
 * 
 * 此函数负责打开一个文件，并将其用于双重写入缓冲的持久化存储。如果缓冲区已经关联了一个打开的文件描述符，
 * 则函数会返回错误，表明缓冲区已经打开。如果文件打开或创建失败，也会返回错误。
 * 
 * @param filename 要打开或创建的文件名。
 * @return 返回结果代码，指示操作是否成功。
 */
RC DiskDoubleWriteBuffer::open_file(const char *filename)
{
  // 检查缓冲区是否已经打开，如果是，则记录错误并返回。
  if (file_desc_ >= 0) {
    LOG_ERROR("Double write buffer has already opened. file desc=%d", file_desc_);
    return RC::BUFFERPOOL_OPEN;
  }
  
  // 尝试打开或创建一个文件，使用读写权限，并设置文件的访问和修改权限。
  int fd = open(filename, O_CREAT | O_RDWR, 0644);
  // 如果文件打开或创建失败，记录错误信息并返回。
  if (fd < 0) {
    LOG_ERROR("Failed to open or creat %s, due to %s.", filename, strerror(errno));
    return RC::SCHEMA_DB_EXIST;
  }

  // 将文件描述符保存到实例变量中，以便后续操作使用。
  file_desc_ = fd;
  // 加载文件中的页面到缓冲区，并返回操作结果。
  return load_pages();
}

// 刷新双重写入缓冲区中的所有页面到磁盘
RC DiskDoubleWriteBuffer::flush_page()
{
  // 同步缓冲区，确保所有数据准备好
  sync();

  // 遍历缓冲区中的所有页面
  for (const auto &pair : dblwr_pages_) {
    // 将页面写入磁盘
    RC rc = write_page(pair.second);
    // 如果写入失败，返回错误代码
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 标记页面为无效，准备删除
    pair.second->valid = false;
    // 执行内部写入逻辑，可能包括更新元数据等
    write_page_internal(pair.second);
    // 释放页面对象的内存
    delete pair.second;
  }

  // 清空页面缓冲区
  dblwr_pages_.clear();
  // 重置头部信息中的页面计数为0
  header_.page_cnt = 0;

  // 成功完成页面刷新
  return RC::SUCCESS;
}

// 向双重写入缓冲区中添加页面
// 参数 bp: 盘缓冲池指针，用于访问缓冲池的属性和方法
// 参数 page_num: 页面编号，用于唯一标识一个页面
// 参数 page: 待添加的页面对象，包含页面数据和元信息
// 返回值: RC 类型，表示操作的成功或失败
RC DiskDoubleWriteBuffer::add_page(DiskBufferPool *bp, PageNum page_num, Page &page)
{
  // 加锁以保证线程安全
  scoped_lock lock_guard(lock_);

  // 构造页面的唯一键值，用于在双重写入缓冲区中查找页面
  DoubleWritePageKey key{bp->id(), page_num};

  // 尝试在双重写入缓冲区中查找页面
  auto iter = dblwr_pages_.find(key);
  if (iter != dblwr_pages_.end()) {
    // 如果找到页面，则更新页面内容
    iter->second->page = page;
    // 记录日志
    LOG_TRACE("[cache hit]add page into double write buffer. buffer_pool_id:%d,page_num:%d,lsn=%d, dwb size=%d",
              bp->id(), page_num, page.lsn, static_cast<int>(dblwr_pages_.size()));
    // 调用内部方法写入页面
    return write_page_internal(iter->second);
  }

  // 如果未找到页面，则创建新的双重写入页面对象
  int64_t          page_cnt   = dblwr_pages_.size();
  DoubleWritePage *dblwr_page = new DoubleWritePage(bp->id(), page_num, page_cnt, page);
  // 将新的页面对象插入到双重写入缓冲区中
  dblwr_pages_.insert(std::pair<DoubleWritePageKey, DoubleWritePage *>(key, dblwr_page));
  // 记录日志
  LOG_TRACE("insert page into double write buffer. buffer_pool_id:%d,page_num:%d,lsn=%d, dwb size:%d",
            bp->id(), page_num, page.lsn, static_cast<int>(dblwr_pages_.size()));

  // 调用内部方法写入页面
  RC rc = write_page_internal(dblwr_page);
  if (OB_FAIL(rc)) {
    // 如果写入失败，记录日志并返回错误码
    LOG_WARN("failed to write page into double write buffer. rc=%s buffer_pool_id:%d,page_num:%d,lsn=%d.",
        strrc(rc), bp->id(), page_num, page.lsn);
    return rc;
  }

  // 检查并更新双重写入缓冲区的页面计数
  if (page_cnt + 1 > header_.page_cnt) {
    header_.page_cnt = page_cnt + 1;
    // 重新定位文件描述符到文件开始位置
    if (lseek(file_desc_, 0, SEEK_SET) == -1) {
      LOG_ERROR("Failed to add page header due to failed to seek %s.", strerror(errno));
      return RC::IOERR_SEEK;
    }

    // 更新文件头部信息
    if (writen(file_desc_, &header_, sizeof(header_)) != 0) {
      LOG_ERROR("Failed to add page header due to %s.", strerror(errno));
      return RC::IOERR_WRITE;
    }
  }

  // 检查双重写入缓冲区的大小是否达到阈值，如果达到则刷新页面到磁盘
  if (static_cast<int>(dblwr_pages_.size()) >= max_pages_) {
    RC rc = flush_page();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush pages in double write buffer");
      return rc;
    }
  }

  // 返回成功
  return RC::SUCCESS;
}

/**
 * 写入双写缓冲区页面的内部实现
 * 
 * 该函数负责将给定的DoubleWritePage对象写入到磁盘上的双写缓冲区中它首先计算页面在文件中的偏移量，
 * 然后尝试将页面写入到该位置函数处理两个主要的错误情况：一个是文件定位（lseek）失败，另一个是实际写入操作（writen）失败
 * 
 * @param page 指向要写入的DoubleWritePage对象的指针
 * @return 返回结果代码，表示操作成功（RC::SUCCESS）或遇到的特定错误（如RC::IOERR_SEEK或RC::IOERR_WRITE）
 */
RC DiskDoubleWriteBuffer::write_page_internal(DoubleWritePage *page)
{
  // 计算页面在文件中的偏移量
  int32_t page_index = page->page_index;
  int64_t offset = page_index * DoubleWritePage::SIZE + DoubleWriteBufferHeader::SIZE;

  // 将文件描述符定位到写入位置如果定位失败，则记录错误并返回相应的错误代码
  if (lseek(file_desc_, offset, SEEK_SET) == -1) {
    LOG_ERROR("Failed to add page %lld of %d due to failed to seek %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_SEEK;
  }

  // 尝试将页面写入文件如果写入失败，则记录错误并返回相应的错误代码
  if (writen(file_desc_, page, DoubleWritePage::SIZE) != 0) {
    LOG_ERROR("Failed to add page %lld of %d due to %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_WRITE;
  }

  // 操作成功，返回成功代码
  return RC::SUCCESS;
}

/**
 * Writes a page to the disk using the double write buffer mechanism.
 * 
 * @param dblwr_page The page to be written, containing both the page data and its metadata.
 * @return Returns the result of the write operation, indicating success or the specific error encountered.
 * 
 * This function first checks if the page to be written is valid. If not, it logs a trace message and returns success,
 * as there is no actual write needed for an invalid page. This step avoids unnecessary disk I/O operations.
 * 
 * If the page is valid, it attempts to retrieve the corresponding disk buffer pool based on the buffer pool ID.
 * Failure to obtain the disk buffer pool results in an assertion failure, as this indicates a serious error in the system.
 * 
 * After successfully obtaining the disk buffer pool, it logs a trace message detailing the page information,
 * then proceeds to call the write_page method of the disk buffer pool to perform the actual page write operation.
 */
RC DiskDoubleWriteBuffer::write_page(DoubleWritePage *dblwr_page)
{
  DiskBufferPool *disk_buffer = nullptr;
  // skip invalid page
  if (!dblwr_page->valid) {
    LOG_TRACE("double write buffer write page invalid. buffer_pool_id:%d,page_num:%d,lsn=%d",
              dblwr_page->key.buffer_pool_id, dblwr_page->key.page_num, dblwr_page->page.lsn);
    return RC::SUCCESS;
  }
  RC rc = bp_manager_.get_buffer_pool(dblwr_page->key.buffer_pool_id, disk_buffer);
  ASSERT(OB_SUCC(rc) && disk_buffer != nullptr, "failed to get disk buffer pool of %d", dblwr_page->key.buffer_pool_id);

  LOG_TRACE("double write buffer write page. buffer_pool_id:%d,page_num:%d,lsn=%d",
            dblwr_page->key.buffer_pool_id, dblwr_page->key.page_num, dblwr_page->page.lsn);

  return disk_buffer->write_page(dblwr_page->key.page_num, dblwr_page->page);
}

RC DiskDoubleWriteBuffer::read_page(DiskBufferPool *bp, PageNum page_num, Page &page)
{
  scoped_lock lock_guard(lock_);
  DoubleWritePageKey key{bp->id(), page_num};
  auto iter = dblwr_pages_.find(key);
  if (iter != dblwr_pages_.end()) {
    page = iter->second->page;
    LOG_TRACE("double write buffer read page success. bp id=%d, page_num:%d, lsn:%d", bp->id(), page_num, page.lsn);
    return RC::SUCCESS;
  }

  return RC::BUFFERPOOL_INVALID_PAGE_NUM;
}

// 清除与指定缓冲池关联的页面
RC DiskDoubleWriteBuffer::clear_pages(DiskBufferPool *buffer_pool)
{
  // 存储待删除的特殊页面
  vector<DoubleWritePage *> spec_pages;
  
  // 定义一个谓词函数，用于识别和移除与指定缓冲池关联的页面
  auto remove_pred = [&spec_pages, buffer_pool](const pair<DoubleWritePageKey, DoubleWritePage *> &pair) {
    DoubleWritePage *dbl_page = pair.second;
    // 如果页面关联的缓冲池ID与指定的缓冲池ID匹配，则将页面添加到待删除列表并返回true
    if (buffer_pool->id() == dbl_page->key.buffer_pool_id) {
      spec_pages.push_back(dbl_page);
      return true;
    }
    return false;
  };

  // 锁定以确保线程安全
  lock_.lock();
  // 使用谓词函数移除所有匹配的页面
  erase_if(dblwr_pages_, remove_pred);
  lock_.unlock();

  // 记录清除的页面数量
  LOG_INFO("clear pages in double write buffer. file name=%s, page count=%d",
           buffer_pool->filename(), spec_pages.size());

  // 页面从小到大排序，防止出现小页面还没有写入，而页面编号更大的seek失败的情况
  sort(spec_pages.begin(), spec_pages.end(), [](DoubleWritePage *a, DoubleWritePage *b) {
    return a->key.page_num < b->key.page_num;
  });

  RC rc = RC::SUCCESS;
  // 遍历所有待删除的页面，将它们写回到磁盘
  for (DoubleWritePage *dbl_page : spec_pages) {
    rc = buffer_pool->write_page(dbl_page->key.page_num, dbl_page->page);
    // 如果写入失败，记录错误并中断循环
    if (OB_FAIL(rc)) {
      LOG_WARN("Failed to write page %s:%d to disk buffer pool. rc=%s",
               buffer_pool->filename(), dbl_page->key.page_num, strrc(rc));
      break;
    }
    // 标记页面为无效，并在内部将其删除
    dbl_page->valid = false;
    write_page_internal(dbl_page);
  }

  // 释放所有待删除页面的内存
  for_each(spec_pages.begin(), spec_pages.end(), [](DoubleWritePage *dbl_page) { delete dbl_page; });

  return RC::SUCCESS;
}

/**
 * 从磁盘加载双写缓冲区页面到内存中。
 * 
 * 此函数首先检查文件描述符是否有效，如果无效则返回错误。
 * 然后检查双写缓冲区是否为空，如果不为空则返回错误。
 * 接着将文件指针定位到文件开头，如果失败则返回错误。
 * 之后读取双写缓冲区的头部信息，如果读取失败则返回错误。
 * 最后根据头部信息中的页面数量，循环读取每个页面到内存中，
 * 并计算校验和以验证数据的完整性。如果校验和匹配，则将页面
 * 插入到双写缓冲区中。如果读取页面失败，则返回错误。
 * 
 * @return RC::SUCCESS 如果加载成功，否则返回相应的错误代码。
 */
RC DiskDoubleWriteBuffer::load_pages()
{
  // 检查文件描述符是否有效
  if (file_desc_ < 0) {
    LOG_ERROR("Failed to load pages, due to file desc is invalid.");
    return RC::BUFFERPOOL_OPEN;
  }

  // 检查双写缓冲区是否为空
  if (!dblwr_pages_.empty()) {
    LOG_ERROR("Failed to load pages, due to double write buffer is not empty. opened?");
    return RC::BUFFERPOOL_OPEN;
  }

  // 将文件指针定位到文件开头
  if (lseek(file_desc_, 0, SEEK_SET) == -1) {
    LOG_ERROR("Failed to load page header, due to failed to lseek:%s.", strerror(errno));
    return RC::IOERR_SEEK;
  }

  // 读取双写缓冲区的头部信息
  int ret = readn(file_desc_, &header_, sizeof(header_));
  if (ret != 0 && ret != -1) {
    LOG_ERROR("Failed to load page header, file_desc:%d, due to failed to read data:%s, ret=%d",
                file_desc_, strerror(errno), ret);
    return RC::IOERR_READ;
  }

  // 循环读取每个页面到内存中
  for (int page_num = 0; page_num < header_.page_cnt; page_num++) {
    int64_t offset = ((int64_t)page_num) * DoubleWritePage::SIZE + DoubleWriteBufferHeader::SIZE;

    // 定位到页面的起始位置
    if (lseek(file_desc_, offset, SEEK_SET) == -1) {
      LOG_ERROR("Failed to load page %d, offset=%ld, due to failed to lseek:%s.", page_num, offset, strerror(errno));
      return RC::IOERR_SEEK;
    }

    // 读取页面数据
    auto dblwr_page = make_unique<DoubleWritePage>();
    Page &page     = dblwr_page->page;
    page.check_sum = (CheckSum)-1;

    ret = readn(file_desc_, dblwr_page.get(), DoubleWritePage::SIZE);
    if (ret != 0) {
      LOG_ERROR("Failed to load page, file_desc:%d, page num:%d, due to failed to read data:%s, ret=%d, page count=%d",
                file_desc_, page_num, strerror(errno), ret, page_num);
      return RC::IOERR_READ;
    }

    // 计算并验证校验和
    const CheckSum check_sum = crc32(page.data, BP_PAGE_DATA_SIZE);
    if (check_sum == page.check_sum) {
      DoubleWritePageKey key = dblwr_page->key;
      dblwr_pages_.insert(pair<DoubleWritePageKey, DoubleWritePage *>(key, dblwr_page.release()));
    } else {
      LOG_TRACE("got a page with an invalid checksum. on disk:%d, in memory:%d", page.check_sum, check_sum);
    }
  }

  // 加载完成后记录日志
  LOG_INFO("double write buffer load pages done. page num=%d", dblwr_pages_.size());
  return RC::SUCCESS;
}

RC DiskDoubleWriteBuffer::recover()
{
  return flush_page();
}

////////////////////////////////////////////////////////////////
RC VacuousDoubleWriteBuffer::add_page(DiskBufferPool *bp, PageNum page_num, Page &page)
{
  return bp->write_page(page_num, page);
}

