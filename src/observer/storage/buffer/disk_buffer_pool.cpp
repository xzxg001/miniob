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
#include <errno.h>
#include <string.h>

#include "common/io/io.h"
#include "common/lang/mutex.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/math/crc.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/buffer/buffer_pool_log.h"
#include "storage/db/db.h"

using namespace common;

static const int MEM_POOL_ITEM_NUM = 20;

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief 将BPFileHeader对象的当前状态转换为字符串表示
 * 
 * 此函数用于调试和日志记录，通过组合页数和分配的页数来创建一个描述性的字符串
 * 它使用ostringstream来构建包含这些信息的字符串
 * 
 * @return string 包含页数和分配页数信息的字符串
 */
string BPFileHeader::to_string() const
{
  // 使用ostringstream构建描述性的字符串
  stringstream ss;
  ss << "pageCount:" << page_count << ", allocatedCount:" << allocated_pages;
  // 返回构建好的字符串
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////

// 构造函数：初始化BPFrameManager类的实例
// 参数name：为帧管理器指定一个名称，用于内部资源的标识和管理
BPFrameManager::BPFrameManager(const char *name) : allocator_(name) {}

/**
 * 初始化帧管理器
 * 
 * 此函数负责初始化BPFrameManager类中的资源分配器，为内存池分配必要的资源
 * 它是BPFrameManager类成功运行的前提条件
 * 
 * @param pool_num 内存池的数量，用于指示需要初始化的内存池数目
 * @return RC 初始化结果，成功返回RC::SUCCESS，内存不足时返回RC::NOMEM
 */
RC BPFrameManager::init(int pool_num)
{
  // 初始化资源分配器，传入false表示不使用特定的分配策略，pool_num指定内存池数量
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    // 资源分配器成功初始化，返回成功状态码
    return RC::SUCCESS;
  }
  // 资源分配器初始化失败，通常是因为内存不足，返回相应的错误状态码
  return RC::NOMEM;
}

/**
 * 清理帧管理器资源
 * 
 * 此函数负责清理帧管理器所占用的资源在调用此函数时，如果帧管理器中仍存在未处理的帧，
 * 则表明内部状态不一致，可能是因为程序逻辑错误或意外的程序中断在这种情况下，
 * 函数将返回INTERNAL错误码，表示帧管理器的内部状态有问题
 * 
 * 如果没有未处理的帧，函数将销毁帧管理器并返回SUCCESS，表示资源成功释放
 * 
 * @return RC 返回结果码，INTERNAL表示内部状态有问题，SUCCESS表示资源成功释放
 */
RC BPFrameManager::cleanup()
{
  // 检查是否存在未处理的帧
  if (frames_.count() > 0) {
    // 如果存在未处理的帧，返回INTERNAL错误码
    return RC::INTERNAL;
  }

  // 销毁帧管理器
  frames_.destroy();
  // 资源成功释放，返回SUCCESS错误码
  return RC::SUCCESS;
}

/**
 * Purges a specified number of frames.
 * 
 * This function is designed to purge a certain number of frames from the frame manager. It first identifies which frames can be purged,
 * then calls the provided purger function to perform the actual purge operation. This function is thread-safe, as it locks the frame
 * manager during the purge process to prevent concurrent modifications.
 * 
 * @param count The number of frames to purge. If less than or equal to 0, it defaults to 1.
 * @param purger A function object that defines how to purge a frame. Returns a Return Code (RC) indicating success or failure.
 * @return The number of frames successfully purged.
 */
int BPFrameManager::purge_frames(int count, function<RC(Frame *frame)> purger)
{
  // Ensure thread safety by locking the frame manager.
  lock_guard<mutex> lock_guard(lock_);

  // Prepare a container for frames that can be purged.
  vector<Frame *> frames_can_purge;
  // Validate the input count, defaulting to 1 if it's less than or equal to 0.
  if (count <= 0) {
    count = 1;
  }
  // Reserve space in advance to minimize memory reallocations.
  frames_can_purge.reserve(count);

  // Define a lambda expression to identify frames that can be purged.
  auto purge_finder = [&frames_can_purge, count](const FrameId &frame_id, Frame *const frame) {
    // Check if the current frame can be purged.
    if (frame->can_purge()) {
      // Temporarily pin the frame to prevent it from being accessed or modified by other threads.
      frame->pin();
      // Add the frame to the list of frames to be purged.
      frames_can_purge.push_back(frame);
      // If enough frames to be purged have been found, stop looking.
      if (frames_can_purge.size() >= static_cast<size_t>(count)) {
        return false;  // false to break the progress
      }
    }
    return true;  // true continue to look up
  };

  // Traverse all frames in reverse order to find frames that can be purged.
  frames_.foreach_reverse(purge_finder);
  // Log the total number of frames found for purging.
  LOG_INFO("purge frames find %ld pages total", frames_can_purge.size());

  // Note: The frame manager is still locked at this point, and purger is a time-consuming operation.
  // It needs to flush dirty page data to disk, which can significantly reduce concurrency.
  int freed_count = 0;
  // Iterate through all frames that can be purged and attempt to purge them.
  for (Frame *frame : frames_can_purge) {
    // Call the purger function to perform the purge operation.
    RC rc = purger(frame);
    // If the purge is successful, free the frame internally.
    if (RC::SUCCESS == rc) {
      free_internal(frame->frame_id(), frame);
      freed_count++;
    } else {
      // If the purge fails, unpin the frame and log a warning.
      frame->unpin();
      LOG_WARN("failed to purge frame. frame_id=%s, rc=%s", 
               frame->frame_id().to_string().c_str(), strrc(rc));
    }
  }
  // Log the total number of frames successfully purged.
  LOG_INFO("purge frame done. number=%d", freed_count);
  // Return the number of frames successfully purged.
  return freed_count;
}

/**
 * 获取指定页面号的帧对象
 * 
 * 此函数通过给定的缓冲池ID和页面号来获取对应的帧对象它首先创建一个帧ID对象，
 * 然后通过互斥锁来保证线程安全，在保护区内调用get_internal函数来获取帧对象
 * 
 * @param buffer_pool_id 缓冲池ID，用于标识不同的缓冲池
 * @param page_num 页面号，用于在缓冲池中定位特定的帧
 * @return Frame* 返回指向所请求帧对象的指针如果找不到该帧，则返回nullptr
 */
Frame *BPFrameManager::get(int buffer_pool_id, PageNum page_num)
{
  // 创建一个帧ID对象，用于唯一标识一个帧
  FrameId frame_id(buffer_pool_id, page_num);

  // 使用lock_guard和mutex来确保线程安全，防止多个线程同时访问get_internal函数
  lock_guard<mutex> lock_guard(lock_);
  // 调用内部函数get_internal来获取帧对象
  return get_internal(frame_id);
}

/**
 * @brief 获取内部帧对象
 * 
 * 本函数通过帧ID从帧管理器中获取对应的帧对象如果帧对象存在，则将其标记为被使用（pin），以防止被回收
 * 
 * @param frame_id 帧的唯一标识符
 * @return Frame* 返回指向帧对象的指针，如果未找到则返回nullptr
 */
Frame *BPFrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  // 尝试从帧管理器中获取指定ID的帧对象
  (void)frames_.get(frame_id, frame);
  // 如果成功获取到帧对象，则将其标记为被使用
  if (frame != nullptr) {
    frame->pin();
  }
  // 返回获取到的帧对象指针，如果没有找到则返回nullptr
  return frame;
}

/**
 * 分配一个帧对象。
 * 
 * 此函数首先尝试从内部缓存中获取指定页码的帧对象。如果找不到，则从分配器中请求一个新的帧对象。
 * 如果成功获取帧对象，则将其与指定的缓冲池ID和页码关联，并将其添加到帧缓存中。
 * 
 * @param buffer_pool_id 缓冲池ID，用于标识帧对象所属的缓冲池。
 * @param page_num 页码，用于标识帧对象内的页码。
 * @return 返回分配的帧对象指针，如果无法分配，则返回nullptr。
 */
Frame *BPFrameManager::alloc(int buffer_pool_id, PageNum page_num)
{
  // 创建帧ID，用于唯一标识帧对象。
  FrameId frame_id(buffer_pool_id, page_num);

  // 加锁，确保线程安全。
  lock_guard<mutex> lock_guard(lock_);

  // 尝试从内部缓存中获取指定页码的帧对象。
  Frame                      *frame = get_internal(frame_id);
  if (frame != nullptr) {
    // 如果找到帧对象，则直接返回。
    return frame;
  }

  // 如果缓存中没有找到帧对象，则从分配器中请求一个新的帧对象。
  frame = allocator_.alloc();
  if (frame != nullptr) {
    // 确保新分配的帧对象的pin计数为0，以验证其有效性。
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s", 
           frame->to_string().c_str());
    // 设置帧对象的缓冲池ID和页码，并将其pin住。
    frame->set_buffer_pool_id(buffer_pool_id);
    frame->set_page_num(page_num);
    frame->pin();
    // 将帧对象添加到帧缓存中。
    frames_.put(frame_id, frame);
  }
  // 返回分配的帧对象，如果无法分配，则返回nullptr。
  return frame;
}

/**
 * 释放指定的帧
 * 
 * 该函数通过给定的缓冲池ID和页号来释放对应的帧它首先构造一个FrameId对象，
 * 然后通过调用free_internal函数来执行实际的释放操作函数使用了锁，以确保线程安全
 * 
 * @param buffer_pool_id 缓冲池ID，用于标识特定的缓冲池
 * @param page_num 页号，用于在缓冲池中标识特定的帧
 * @param frame 指向帧的指针，如果为nullptr，则根据buffer_pool_id和page_num来确定帧
 * 
 * @return 返回释放操作的结果，指示释放是否成功
 */
RC BPFrameManager::free(int buffer_pool_id, PageNum page_num, Frame *frame)
{
  // 构造FrameId对象，用于后续的帧释放操作
  FrameId frame_id(buffer_pool_id, page_num);

  // 使用锁，以确保在释放帧的过程中线程安全
  lock_guard<mutex> lock_guard(lock_);
  // 调用内部函数free_internal来执行实际的帧释放操作
  return free_internal(frame_id, frame);
}

/**
 * 释放内部帧
 * 
 * 本函数用于释放一个内部帧，确保帧被正确回收并可用于后续的分配
 * 
 * @param frame_id 帧的唯一标识符
 * @param frame 指向帧的指针，用于验证帧的合法性
 * 
 * @return RC::SUCCESS 如果帧成功释放
 */
RC BPFrameManager::free_internal(const FrameId &frame_id, Frame *frame)
{
  // 尝试从帧管理器中获取指定ID的帧
  Frame                *frame_source = nullptr;
  [[maybe_unused]] bool found        = frames_.get(frame_id, frame_source);
  
  // 断言：确保找到的帧与传入的帧指针一致，并且帧的引用计数为1
  // 这是为了确保帧是合法的并且可以被安全地释放
  ASSERT(found && frame == frame_source && frame->pin_count() == 1,
      "failed to free frame. found=%d, frameId=%s, frame_source=%p, frame=%p, pinCount=%d, lbt=%s",
      found, frame_id.to_string().c_str(), frame_source, frame, frame->pin_count(), lbt());

  // 重置帧的页码为-1，表示该帧不再关联任何页
  frame->set_page_num(-1);
  
  // 解除帧的锁定，使其可以被再次使用
  frame->unpin();
  
  // 从帧管理器中移除该帧的记录
  frames_.remove(frame_id);
  
  // 将帧返回到分配器，以便后续的分配
  allocator_.free(frame);
  
  // 成功释放帧
  return RC::SUCCESS;
}

/**
 * 根据缓冲池ID查找帧列表
 * 
 * 该函数的目标是通过缓冲池ID找到所有关联的帧，并将它们以列表的形式返回
 * 它首先锁定互斥锁以确保线程安全，然后遍历所有帧，检查它们是否属于指定的缓冲池ID
 * 如果是，它会将帧添加到要返回的帧列表中在添加到列表之前，帧会被固定（pin）
 * 
 * @param buffer_pool_id 缓冲池ID，用于查找帧
 * @return 返回一个帧的列表，这些帧都属于指定的缓冲池ID
 */
list<Frame *> BPFrameManager::find_list(int buffer_pool_id)
{
  // 锁定互斥锁以确保线程安全
  lock_guard<mutex> lock_guard(lock_);

  // 初始化一个空的帧列表，用于存储找到的帧
  list<Frame *> frames;

  // 定义一个lambda函数作为帧遍历的fetcher函数
  auto               fetcher = [&frames, buffer_pool_id](const FrameId &frame_id, Frame *const frame) -> bool {
    // 检查帧是否属于指定的缓冲池ID
    if (buffer_pool_id == frame_id.buffer_pool_id()) {
      // 固定帧以防止它被移除或替换
      frame->pin();
      // 将帧添加到列表中
      frames.push_back(frame);
    }
    // 返回true表示继续遍历
    return true;
  };

  // 使用fetcher函数遍历所有帧
  frames_.foreach (fetcher);

  // 返回找到的帧列表
  return frames;
}

////////////////////////////////////////////////////////////////////////////////
// 构造函数：初始化BufferPoolIterator对象
BufferPoolIterator::BufferPoolIterator() {}
BufferPoolIterator::~BufferPoolIterator() {}
/**
 * 初始化BufferPoolIterator对象。
 * 
 * 该函数用于设置迭代器的起始页面和初始化位图。通过位图，迭代器能够跟踪哪些页面已被分配，
 * 哪些页面是空闲的。此外，根据start_page参数的不同，设置当前页面数的初始值。
 * 
 * @param bp DiskBufferPool的引用，表示磁盘缓冲池。
 * @param start_page 起始页面号，默认为0。如果为0或负数，则当前页面数设为-1；
 *                   否则，设为start_page减1。
 * 
 * @return RC::SUCCESS 表示初始化成功。
 */
RC BufferPoolIterator::init(DiskBufferPool &bp, PageNum start_page /* = 0 */)
{
  // 初始化位图，bitmap_用于跟踪页面的分配情况
  bitmap_.init(bp.file_header_->bitmap, bp.file_header_->page_count);
  
  // 根据start_page参数设置当前页面数的初始值
  if (start_page <= 0) {
    current_page_num_ = -1;
  } else {
    current_page_num_ = start_page - 1;
  }
  
  // 初始化成功，返回成功状态码
  return RC::SUCCESS;
}

/**
 * 判断迭代器是否还有下一个元素。
 *
 * 本函数通过查找当前页面编号之后的下一个已设置的位图位来确定是否有下一个元素。
 * 如果找到下一个已设置的位图位，则表明还有下一个元素；否则，表明没有更多元素。
 *
 * @return 如果还有下一个元素，则返回true；否则返回false。
 */
bool BufferPoolIterator::has_next() { return bitmap_.next_setted_bit(current_page_num_ + 1) != -1; }

/**
 * @brief 获取下一个页面编号
 * 
 * 该函数用于迭代缓冲池中的下一个有效页面。它通过位图(bitmap_)查找当前页面之后的下一个设置为1的位，
 * 即下一个有效页面。如果找到了下一个有效页面，则更新当前页面编号(current_page_num_)为该页面编号，
 * 并返回这个页面编号。如果没有找到，则返回-1。
 * 
 * @return PageNum 下一个有效页面的编号，如果没有找到，则返回-1。
 */
PageNum BufferPoolIterator::next()
{
  // 查找当前页面之后的下一个有效页面
  PageNum next_page = bitmap_.next_setted_bit(current_page_num_ + 1);
  if (next_page != -1) {
    // 更新当前页面编号为下一个有效页面的编号
    current_page_num_ = next_page;
  }
  // 返回下一个有效页面的编号，如果没有找到，则返回-1
  return next_page;
}

// 重置BufferPoolIterator的状态，使其能够从头开始遍历
RC BufferPoolIterator::reset()
{
  // 将当前页码重置为0，即回到迭代器的起始位置
  current_page_num_ = 0;
  // 重置操作成功完成，返回成功状态码
  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////
// 盘缓冲池类的构造函数
// 该构造函数初始化了盘缓冲池类的实例，绑定了缓冲池管理器、帧管理器、双写缓冲区和日志处理器
// 参数:
// - bp_manager: 缓冲池管理器实例，负责缓冲区的分配和管理
// - frame_manager: 帧管理器实例，负责帧的分配和管理
// - dblwr_manager: 双写缓冲区实例，用于提高写入操作的可靠性
// - log_handler: 日志处理器实例，负责记录盘缓冲池的操作日志
DiskBufferPool::DiskBufferPool(
    BufferPoolManager &bp_manager, BPFrameManager &frame_manager, DoubleWriteBuffer &dblwr_manager, LogHandler &log_handler)
    : bp_manager_(bp_manager), frame_manager_(frame_manager), dblwr_manager_(dblwr_manager), log_handler_(*this, log_handler)
{}

// DiskBufferPool类的析构函数
DiskBufferPool::~DiskBufferPool()
{
  // 关闭文件，释放资源
  close_file();
  // 日志记录，表明磁盘缓冲池即将退出
  LOG_INFO("disk buffer pool exit");
}

// 打开缓冲池文件
// 参数: file_name - 文件名
// 返回值: RC - 操作结果，成功或错误类型
RC DiskBufferPool::open_file(const char *file_name)
{
  // 打开文件
  int fd = open(file_name, O_RDWR);
  if (fd < 0) {
    // 打开文件失败，记录错误日志并返回访问错误
    LOG_ERROR("Failed to open file %s, because %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }
  // 打开文件成功，记录信息日志
  LOG_INFO("Successfully open buffer pool file %s.", file_name);

  // 保存文件名和文件描述符
  file_name_ = file_name;
  file_desc_ = fd;

  // 读取文件的第一页（头页）
  Page header_page;
  int ret = readn(file_desc_, &header_page, sizeof(header_page));
  if (ret != 0) {
    // 读取头页失败，记录错误日志，关闭文件，并返回读取错误
    LOG_ERROR("Failed to read first page of %s, due to %s.", file_name, strerror(errno));
    close(fd);
    file_desc_ = -1;
    return RC::IOERR_READ;
  }

  // 解析头页中的文件头信息
  BPFileHeader *tmp_file_header = reinterpret_cast<BPFileHeader *>(header_page.data);
  buffer_pool_id_ = tmp_file_header->buffer_pool_id;

  // 为头页分配帧
  RC rc = allocate_frame(BP_HEADER_PAGE, &hdr_frame_);
  if (rc != RC::SUCCESS) {
    // 分配帧失败，记录错误日志，关闭文件，并返回错误码
    LOG_ERROR("failed to allocate frame for header. file name %s", file_name_.c_str());
    close(fd);
    file_desc_ = -1;
    return rc;
  }

  // 设置帧的缓冲池ID并标记为已访问
  hdr_frame_->set_buffer_pool_id(id());
  hdr_frame_->access();

  // 加载头页到帧中
  if ((rc = load_page(BP_HEADER_PAGE, hdr_frame_)) != RC::SUCCESS) {
    // 加载头页失败，记录错误日志，清除帧，关闭文件，并返回错误码
    LOG_ERROR("Failed to load first page of %s, due to %s.", file_name, strerror(errno));
    purge_frame(BP_HEADER_PAGE, hdr_frame_);
    close(fd);
    file_desc_ = -1;
    return rc;
  }

  // 获取文件头指针
  file_header_ = (BPFileHeader *)hdr_frame_->data();

  // 打开文件成功，记录信息日志，并返回成功
  LOG_INFO("Successfully open %s. file_desc=%d, hdr_frame=%p, file header=%s",
           file_name, file_desc_, hdr_frame_, file_header_->to_string().c_str());
  return RC::SUCCESS;
}

// 关闭与缓冲池关联的文件
RC DiskBufferPool::close_file()
{
  RC rc = RC::SUCCESS;
  // 如果文件描述符小于0，则视为文件未打开，直接返回成功
  if (file_desc_ < 0) {
    return rc;
  }

  // 解除对头块的固定，使其可以被写入或替换
  hdr_frame_->unpin();

  // TODO: 理论上是在回放时回滚未提交事务，但目前没有undo log，因此不下刷数据page，只通过redo log回放
  // 尝试清除所有页面，如果失败，则记录错误并返回
  rc = purge_all_pages();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to close %s, due to failed to purge pages. rc=%s", file_name_.c_str(), strrc(rc));
    return rc;
  }

  // 清除双写缓冲区中的所有页面，如果失败，则记录警告并返回
  rc = dblwr_manager_.clear_pages(this);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to clear pages in double write buffer. filename=%s, rc=%s", file_name_.c_str(), strrc(rc));
    return rc;
  }

  // 清空已处理页面的记录
  disposed_pages_.clear();

  // 尝试关闭文件描述符，如果失败，则记录错误并返回IO错误代码
  if (close(file_desc_) < 0) {
    LOG_ERROR("Failed to close fileId:%d, fileName:%s, error:%s", file_desc_, file_name_.c_str(), strerror(errno));
    return RC::IOERR_CLOSE;
  }
  // 记录成功关闭文件的日志
  LOG_INFO("Successfully close file %d:%s.", file_desc_, file_name_.c_str());
  // 将文件描述符标记为未打开
  file_desc_ = -1;

  // 通知缓冲池管理器文件已关闭
  bp_manager_.close_file(file_name_.c_str());
  return RC::SUCCESS;
}

// Retrieves a specified page from the buffer pool
// If the page is already in the buffer pool, it is directly returned; otherwise, a frame is allocated and the page is loaded from the disk
// Parameters:
//   page_num: The number of the page to retrieve
//   frame: Pointer to the frame where the page is stored, output parameter
// Return value:
//   RC::SUCCESS: Operation successful
//   Other return codes: Operation failed, specific reason see RC enumeration
RC DiskBufferPool::get_this_page(PageNum page_num, Frame **frame)
{
  RC rc  = RC::SUCCESS;
  *frame = nullptr;

  // Try to get the frame of the page from the frame manager
  Frame *used_match_frame = frame_manager_.get(id(), page_num);
  if (used_match_frame != nullptr) {
    // If the page is already in the buffer pool, update its access time and return the frame
    used_match_frame->access();
    *frame = used_match_frame;
    return RC::SUCCESS;
  }

  // Lock the buffer pool to ensure thread safety during frame allocation and page loading
  scoped_lock lock_guard(lock_);  // Directly add a global lock, which can be optimized by using a finer-grained lock based on the accessed page to improve parallelism

  // Allocate one page and load the data into this page
  Frame *allocated_frame = nullptr;

  // Allocate a frame for the page
  rc = allocate_frame(page_num, &allocated_frame);
  if (rc != RC::SUCCESS) {
    // If frame allocation fails, log an error message and return the error code
    LOG_ERROR("Failed to alloc frame %s:%d, due to failed to alloc page.", file_name_.c_str(), page_num);
    return rc;
  }

  // Set the buffer pool ID for the allocated frame
  allocated_frame->set_buffer_pool_id(id());
  // allocated_frame->pin(); // The pin operation is already performed in manager::get
  // Update the access time of the frame
  allocated_frame->access();

  // Load the page data into the allocated frame
  if ((rc = load_page(page_num, allocated_frame)) != RC::SUCCESS) {
    // If page loading fails, log an error message, purge the frame, and return the error code
    LOG_ERROR("Failed to load page %s:%d", file_name_.c_str(), page_num);
    purge_frame(page_num, allocated_frame);
    return rc;
  }

  // Return the frame pointer of the loaded page
  *frame = allocated_frame;
  return RC::SUCCESS;
}

// Allocate a new page in the buffer pool
RC DiskBufferPool::allocate_page(Frame **frame)
{
  RC rc = RC::SUCCESS;

  // Lock to ensure thread safety
  lock_.lock();

  int byte = 0, bit = 0;
  // Check if there are unallocated pages
  if ((file_header_->allocated_pages) < (file_header_->page_count)) {
    // There is one free page
    for (int i = 0; i < file_header_->page_count; i++) {
      byte = i / 8;
      bit  = i % 8;
      // Find an unallocated page
      if (((file_header_->bitmap[byte]) & (1 << bit)) == 0) {
        (file_header_->allocated_pages)++;
        file_header_->bitmap[byte] |= (1 << bit);
        // TODO,  do we need clean the loaded page's data?
        hdr_frame_->mark_dirty();
        LSN lsn = 0;
        // Log the allocation operation
        rc = log_handler_.allocate_page(i, lsn);
        if (OB_FAIL(rc)) {
          LOG_ERROR("Failed to log allocate page %d, rc=%s", i, strrc(rc));
          // 忽略了错误
        }

        hdr_frame_->set_lsn(lsn);

        // Unlock and return the allocated page
        lock_.unlock();
        return get_this_page(i, frame);
      }
    }
  }

  // Check if the buffer pool is full
  if (file_header_->page_count >= BPFileHeader::MAX_PAGE_NUM) {
    LOG_WARN("file buffer pool is full. page count %d, max page count %d",
        file_header_->page_count, BPFileHeader::MAX_PAGE_NUM);
    lock_.unlock();
    return RC::BUFFERPOOL_NOBUF;
  }

  LSN lsn = 0;
  // Log the allocation operation for new pages
  rc = log_handler_.allocate_page(file_header_->page_count, lsn);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log allocate page %d, rc=%s", file_header_->page_count, strrc(rc));
    // 忽略了错误
  }
  hdr_frame_->set_lsn(lsn);

  // Allocate a new frame for the new page
  PageNum page_num        = file_header_->page_count;
  Frame  *allocated_frame = nullptr;
  if ((rc = allocate_frame(page_num, &allocated_frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to allocate frame %s, due to no free page.", file_name_.c_str());
    lock_.unlock();
    return rc;
  }

  LOG_INFO("allocate new page. file=%s, pageNum=%d, pin=%d",
           file_name_.c_str(), page_num, allocated_frame->pin_count());

  // Update the allocation information
  file_header_->allocated_pages++;
  file_header_->page_count++;

  byte = page_num / 8;
  bit  = page_num % 8;
  file_header_->bitmap[byte] |= (1 << bit);
  hdr_frame_->mark_dirty();

  // Initialize the newly allocated frame
  allocated_frame->set_buffer_pool_id(id());
  allocated_frame->access();
  allocated_frame->clear_page();
  allocated_frame->set_page_num(file_header_->page_count - 1);

  // Use flush operation to extension file
  if ((rc = flush_page_internal(*allocated_frame)) != RC::SUCCESS) {
    LOG_WARN("Failed to alloc page %s , due to failed to extend one page.", file_name_.c_str());
    // skip return false, delay flush the extended page
    // return tmp;
  }

  // Unlock and return the newly allocated frame
  lock_.unlock();

  *frame = allocated_frame;
  return RC::SUCCESS;
}

/**
 * DiskBufferPool类中的dispose_page方法用于回收一个页面。
 * 
 * 该方法首先检查要回收的页面编号是否为0，如果是，则记录错误日志并返回内部错误，
 * 因为第0页是特殊页面，不允许被回收。
 * 
 * 接着，方法使用scoped_lock来确保线程安全，然后尝试从帧管理器中获取与页面编号对应的帧。
 * 如果帧存在，表明该页面当前正在使用中，方法将释放该帧并记录日志。
 * 如果帧不存在，表明该页面当前不在内存中，方法将记录调试日志并继续执行。
 * 
 * 方法随后调用日志处理器的deallocate_page方法来记录页面回收操作，
 * 如果记录失败，方法将记录错误日志，但仍然允许操作继续进行。
 * 
 * 最后，方法更新文件头信息，包括日志序列号、标记文件头为脏、减少已分配页面计数，
 * 并在位图中清除与回收页面对应的位，以反映页面的回收状态。
 * 
 * @param page_num 要回收的页面编号。
 * @return 操作状态，成功返回RC::SUCCESS。
 */
RC DiskBufferPool::dispose_page(PageNum page_num)
{
  // 检查页面编号是否为0，如果是，则记录错误日志并返回内部错误
  if (page_num == 0) {
    LOG_ERROR("Failed to dispose page %d, because it is the first page. filename=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }
  
  // 使用scoped_lock确保线程安全
  scoped_lock lock_guard(lock_);
  
  // 尝试从帧管理器中获取与页面编号对应的帧
  Frame           *used_frame = frame_manager_.get(id(), page_num);
  
  // 如果帧存在，释放该帧并记录日志
  if (used_frame != nullptr) {
    ASSERT("the page try to dispose is in use. frame:%s", used_frame->to_string().c_str());
    frame_manager_.free(id(), page_num, used_frame);
  } else {
    // 如果帧不存在，记录调试日志并继续执行
    LOG_DEBUG("page not found in memory while disposing it. pageNum=%d", page_num);
  }

  // 初始化日志序列号
  LSN lsn = 0;
  
  // 调用日志处理器的deallocate_page方法记录页面回收操作
  RC rc = log_handler_.deallocate_page(page_num, lsn);
  
  // 如果记录失败，记录错误日志，但允许操作继续进行
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log deallocate page %d, rc=%s", page_num, strrc(rc));
    // ignore error handle
  }

  // 更新文件头信息，包括日志序列号、标记文件头为脏、减少已分配页面计数，
  // 并在位图中清除与回收页面对应的位
  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  file_header_->allocated_pages--;
  char tmp = 1 << (page_num % 8);
  file_header_->bitmap[page_num / 8] &= ~tmp;
  
  // 返回成功状态
  return RC::SUCCESS;
}

/**
 * 解除对页面的固定操作
 * 
 * 当不再需要将某个页面固定在内存中时，调用此函数来解除对该页面的固定状态
 * 这允许页面被交换到磁盘，释放内存资源
 * 
 * @param frame 指向要解除固定的页面帧的指针
 * @return 返回操作结果，成功时返回RC::SUCCESS
 */
RC DiskBufferPool::unpin_page(Frame *frame)
{
  // 调用Frame对象的unpin方法来解除页面的固定状态
  frame->unpin();
  
  // 成功解除固定后，返回成功状态码
  return RC::SUCCESS;
}

/**
 * 从磁盘缓冲池中清除指定的帧
 * 
 * 当清除一个帧时，首先检查其引用计数是否为1，因为如果引用计数大于1，
 * 表示该帧正在被使用，不能安全地清除如果帧是脏的，还需要先将其刷新到磁盘
 * 
 * @param page_num 要清除的页号
 * @param buf 指向要清除的帧的指针
 * @return 返回结果代码，如果成功清除则返回RC::SUCCESS，否则返回相应的错误代码
 */
RC DiskBufferPool::purge_frame(PageNum page_num, Frame *buf)
{
  // 检查帧的引用计数是否为1，如果不是，则不能安全地清除
  if (buf->pin_count() != 1) {
    LOG_INFO("Begin to free page %d frame_id=%s, but it's pin count > 1:%d.",
        buf->page_num(), buf->frame_id().to_string().c_str(), buf->pin_count());
    return RC::LOCKED_UNLOCK;
  }

  // 如果帧是脏的，需要先将其刷新到磁盘
  if (buf->dirty()) {
    RC rc = flush_page_internal(*buf);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to flush page %d frame_id=%s during purge page.", buf->page_num(), buf->frame_id().to_string().c_str());
      return rc;
    }
  }

  // 清除帧并释放其资源
  LOG_DEBUG("Successfully purge frame =%p, page %d frame_id=%s", buf, buf->page_num(), buf->frame_id().to_string().c_str());
  frame_manager_.free(id(), page_num, buf);
  return RC::SUCCESS;
}

/**
 * 从磁盘缓冲池中清除指定的页面。
 * 
 * 此函数旨在清除指定页面编号对应的页面。它首先尝试从帧管理器中获取对应的帧，
 * 如果该帧存在，则调用purge_frame函数来清除该帧。如果帧不存在，表示该页面不在缓冲池中，
 * 函数将直接返回成功。
 * 
 * @param page_num 要清除的页面的页面编号。
 * @return 返回清除操作的结果，如果操作成功则返回RC::SUCCESS。
 */
RC DiskBufferPool::purge_page(PageNum page_num)
{
  // 获取锁以确保线程安全。
  scoped_lock lock_guard(lock_);

  // 尝试从帧管理器中获取指定页面编号对应的帧。
  Frame           *used_frame = frame_manager_.get(id(), page_num);
  if (used_frame != nullptr) {
    // 如果帧存在，则调用purge_frame函数来清除该帧。
    return purge_frame(page_num, used_frame);
  }

  // 如果帧不存在，表示该页面不在缓冲池中，直接返回成功。
  return RC::SUCCESS;
}

RC DiskBufferPool::purge_all_pages()
{
  list<Frame *> used = frame_manager_.find_list(id());

  scoped_lock lock_guard(lock_);
  for (list<Frame *>::iterator it = used.begin(); it != used.end(); ++it) {
    Frame *frame = *it;

    purge_frame(frame->page_num(), frame);
  }
  return RC::SUCCESS;
}

/**
 * 检查所有页面是否未被钉住
 * 
 * 此函数的目的是遍历所有帧，确保每个页面都没有被钉住（pin_count为0）
 * 特别地，对于BP_HEADER_PAGE，确保其pin_count不超过1
 * 如果发现任何页面被钉住，将记录警告日志
 * 
 * @return RC::SUCCESS 如果所有页面都未被钉住
 */
RC DiskBufferPool::check_all_pages_unpinned()
{
  // 获取当前缓冲池中所有帧的列表
  list<Frame *> frames = frame_manager_.find_list(id());

  // 加锁以保护帧列表的访问
  scoped_lock lock_guard(lock_);
  for (Frame *frame : frames) {
    // 先取消钉住状态
    frame->unpin();
    // 检查BP_HEADER_PAGE的钉住计数是否超过1
    if (frame->page_num() == BP_HEADER_PAGE && frame->pin_count() > 1) {
      LOG_WARN("This page has been pinned. id=%d, pageNum:%d, pin count=%d",
          id(), frame->page_num(), frame->pin_count());
    } else if (frame->page_num() != BP_HEADER_PAGE && frame->pin_count() > 0) {
      // 检查其他页面的钉住计数是否超过0
      LOG_WARN("This page has been pinned. id=%d, pageNum:%d, pin count=%d",
          id(), frame->page_num(), frame->pin_count());
    }
  }
  // 记录日志：所有页面已检查完毕
  LOG_INFO("all pages have been checked of id %d", id());
  return RC::SUCCESS;
}

RC DiskBufferPool::flush_page(Frame &frame)
{
  scoped_lock lock_guard(lock_);
  return flush_page_internal(frame);
}

// Flushes a page internally in the disk buffer pool.
// This function is responsible for writing the data of a frame to the log, calculating the checksum, 
// and then writing the data to the double write buffer. Finally, it marks the frame as not dirty.
//
// Parameters:
//   frame: The frame object containing the page data to be flushed.
//
// Return value:
//   Returns RC::SUCCESS upon successful execution, otherwise returns the corresponding error code.
RC DiskBufferPool::flush_page_internal(Frame &frame)
{
  // The better way is use mmap the block into memory,
  // so it is easier to flush data to file.

  // Flush the page data to the log.
  RC rc = log_handler_.flush_page(frame.page());
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log flush frame= %s, rc=%s", frame.to_string().c_str(), strrc(rc));
    // ignore error handle
  }

  // Calculate and set the checksum for the frame.
  frame.set_check_sum(crc32(frame.page().data, BP_PAGE_DATA_SIZE));

  // Add the page data to the double write buffer.
  rc = dblwr_manager_.add_page(this, frame.page_num(), frame.page());
  if (OB_FAIL(rc)) {
    return rc;
  }

  // Clear the dirty flag for the frame, indicating that the data has been flushed.
  frame.clear_dirty();
  LOG_DEBUG("Flush block. file desc=%d, frame=%s", file_desc_, frame.to_string().c_str());

  return RC::SUCCESS;
}

/**
 * 刷新所有页面到磁盘
 * 
 * 此函数负责将缓冲池中的所有脏页面刷新到磁盘上这是一个关键操作，确保数据一致性
 * 和持久性，特别是在数据库关闭或系统崩溃之前执行
 * 
 * @return RC 表示操作的成功或失败如果所有页面都成功刷新，则返回RC::SUCCESS；
 *         否则，返回相应的错误代码
 */
RC DiskBufferPool::flush_all_pages()
{
  // 获取当前缓冲池中所有正在使用的帧
  list<Frame *> used = frame_manager_.find_list(id());
  
  // 遍历每个帧，尝试将其刷新到磁盘
  for (Frame *frame : used) {
    // 调用flush_page函数将帧内容刷新到磁盘
    RC rc = flush_page(*frame);
    // 解除帧的固定状态，允许其被替换
    frame->unpin();
    
    // 如果刷新操作失败，记录错误日志并返回错误代码
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to flush all pages");
      return rc;
    }
  }
  
  // 所有页面刷新成功，返回成功代码
  return RC::SUCCESS;
}

/**
 * 从磁盘缓冲池中恢复一个页面。
 * 
 * 本函数通过更新文件头中的位图来恢复指定页面号的页面，标记该页面为已分配并增加已分配页面数。
 * 如果页面已经处于分配状态，则不会进行任何操作。
 * 
 * @param page_num 要恢复的页面的页面号。
 * @return 返回RC::SUCCESS表示操作成功。
 */
RC DiskBufferPool::recover_page(PageNum page_num)
{
  // 计算页面号在位图中的字节位置和位位置。
  int byte = 0, bit = 0;
  byte = page_num / 8;
  bit  = page_num % 8;

  // 加锁以确保线程安全。
  scoped_lock lock_guard(lock_guard);
  // 检查页面是否已经分配。
  if (!(file_header_->bitmap[byte] & (1 << bit))) {
    // 更新位图，标记页面为已分配。
    file_header_->bitmap[byte] |= (1 << bit);
    // 更新文件头中的已分配页面数和页面总数。
    file_header_->allocated_pages++;
    file_header_->page_count++;
    // 标记文件头帧为脏，需要更新到磁盘。
    hdr_frame_->mark_dirty();
  }
  // 恢复页面成功。
  return RC::SUCCESS;
}

/**
 * 将一个页面写入磁盘缓冲池中的指定位置
 * 
 * 此函数通过将给定页面的数据写入到与page_num对应的文件位置来实现页面的持久化
 * 它首先计算页面在文件中的偏移量，然后尝试将页面数据写入该位置
 * 如果在寻找文件位置或写入过程中遇到错误，函数将记录错误信息并返回相应的错误代码
 * 
 * @param page_num 页面编号，用于计算页面在文件中的偏移量
 * @param page 待写入的页面对象，包含页面的数据和元信息
 * @return RC 表示操作的成功或失败，成功时返回RC::SUCCESS，否则返回相应的错误代码
 */
RC DiskBufferPool::write_page(PageNum page_num, Page &page)
{
  // 加锁以确保线程安全
  scoped_lock lock_guard(wr_lock_);
  
  // 计算页面在文件中的偏移量
  int64_t     offset = ((int64_t)page_num) * sizeof(Page);
  
  // 调整文件位置，如果失败则记录错误并返回
  if (lseek(file_desc_, offset, SEEK_SET) == -1) {
    LOG_ERROR("Failed to write page %lld of %d due to failed to seek %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_SEEK;
  }

  // 写入页面数据，如果失败则记录错误并返回
  if (writen(file_desc_, &page, sizeof(Page)) != 0) {
    LOG_ERROR("Failed to write page %lld of %d due to %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_WRITE;
  }

  // 写入成功后，记录日志信息
  LOG_TRACE("write_page: buffer_pool_id:%d, page_num:%d, lsn=%d, check_sum=%d", id(), page_num, page.lsn, page.check_sum);
  
  // 返回成功
  return RC::SUCCESS;
}

/**
 * 在重做日志应用阶段分配数据页
 * 
 * 当前函数旨在重做日志过程中分配指定的数据页如果数据页已经分配或符合分配条件，则返回成功
 * 否则，根据不同的错误情况返回相应的错误代码
 * 
 * @param lsn 日志序列号，表示需要重做到的日志位置
 * @param page_num 需要分配的数据页编号
 * @return RC::SUCCESS 成功分配数据页或数据页已分配
 *         RC::INTERNAL 内部错误，如页面编号不连续或缓冲池已满
 */
RC DiskBufferPool::redo_allocate_page(LSN lsn, PageNum page_num)
{
  // 如果当前头帧的日志序列号大于等于lsn，则无需进行进一步操作
  if (hdr_frame_->lsn() >= lsn) {
    return RC::SUCCESS;
  }

  // scoped_lock lock_guard(lock_); // redo 过程中可以不加锁
  // 检查页面编号是否在文件头记录的页面数量范围内
  if (page_num < file_header_->page_count) {
    // 创建位图对象，用于管理页面的分配状态
    Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
    // 如果指定页面已经分配，则直接返回成功
    if (bitmap.get_bit(page_num)) {
      LOG_WARN("page %d has been allocated. file=%s", page_num, file_name_.c_str());
      return RC::SUCCESS;
    }

    // 分配页面并更新分配计数
    bitmap.set_bit(page_num);
    file_header_->allocated_pages++;
    hdr_frame_->mark_dirty();
    return RC::SUCCESS;
  }

  // 处理页面编号超出文件头记录的页面数量的情况
  if (page_num > file_header_->page_count) {
    LOG_WARN("page %d is not continuous. file=%s, page_count=%d",
             page_num, file_name_.c_str(), file_header_->page_count);
    return RC::INTERNAL;
  }

  // 处理页面编号等于文件头记录的页面数量的情况
  if (file_header_->page_count >= BPFileHeader::MAX_PAGE_NUM) {
    LOG_WARN("file buffer pool is full. page count %d, max page count %d",
        file_header_->page_count, BPFileHeader::MAX_PAGE_NUM);
    return RC::INTERNAL;
  }

  // 更新分配和计数，并设置头帧的日志序列号
  file_header_->allocated_pages++;
  file_header_->page_count++;
  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  
  // TODO 应该检查文件是否足够大，包含了当前新分配的页面

  // 更新位图以反映页面分配状态
  Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
  bitmap.set_bit(page_num);
  LOG_TRACE("[redo] allocate new page. file=%s, pageNum=%d", file_name_.c_str(), page_num);
  return RC::SUCCESS;
}

/**
 * 执行日志重做操作，回收指定的页面
 * 
 * 当前函数通过重做日志操作来回收一个之前分配的页面它首先检查当前缓冲池头部的LSN是否大于等于日志的LSN，
 * 如果是，则无需进行任何操作如果页面编号超出文件中的页面总数，则返回错误如果页面已经被回收，则返回错误
 * 最后，如果一切正常，页面将被标记为未分配，文件头部信息将被更新，缓冲池头部帧将被标记为脏，并记录日志
 * 
 * @param lsn 日志序列号，表示需要重做到哪个版本
 * @param page_num 需要回收的页面编号
 * @return RC::SUCCESS 如果操作成功，否则返回相应的错误码
 */
RC DiskBufferPool::redo_deallocate_page(LSN lsn, PageNum page_num)
{
  // 检查当前头部帧的LSN是否大于等于日志的LSN，如果是，则无需操作
  if (hdr_frame_->lsn() >= lsn) {
    return RC::SUCCESS;
  }

  // 检查页面编号是否超出文件中的页面总数，如果是，则返回错误
  if (page_num >= file_header_->page_count) {
    LOG_WARN("page %d is not exist. file=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }

  // 创建位图对象，用于管理页面的分配状态
  Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
  // 检查页面是否已经被回收，如果是，则返回错误
  if (!bitmap.get_bit(page_num)) {
    LOG_WARN("page %d has been deallocated. file=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }

  // 回收页面，即清除位图中的对应位，并更新文件头部的已分配页面数
  bitmap.clear_bit(page_num);
  file_header_->allocated_pages--;
  // 更新头部帧的LSN，并标记为脏
  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  // 记录日志
  LOG_TRACE("[redo] deallocate page. file=%s, pageNum=%d", file_name_.c_str(), page_num);
  return RC::SUCCESS;
}

/**
 * 分配一个帧并将其绑定到指定的页面编号
 * 如果没有可用的帧，尝试通过 purge 操作释放帧
 * 
 * @param page_num 页面编号，用于分配帧
 * @param buffer 输出参数，分配的帧指针
 * @return RC::SUCCESS 如果成功分配帧，否则返回相应的错误代码
 */
RC DiskBufferPool::allocate_frame(PageNum page_num, Frame **buffer)
{
  // 定义一个 lambda 函数用于刷新帧
  auto purger = [this](Frame *frame) {
    // 如果帧没有被修改，则不需要刷新
    if (!frame->dirty()) {
      return RC::SUCCESS;
    }

    RC rc = RC::SUCCESS;
    // 根据帧所属的缓冲池标识来决定刷新帧的方法
    if (frame->buffer_pool_id() == id()) {
      rc = this->flush_page_internal(*frame);
    } else {
      rc = bp_manager_.flush_page(*frame);
    }

    // 如果刷新失败，记录错误信息
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to aclloc block due to failed to flush old block. rc=%s", strrc(rc));
    }
    return rc;
  };

  while (true) {
    // 尝试分配一个帧
    Frame *frame = frame_manager_.alloc(id(), page_num);
    if (frame != nullptr) {
      *buffer = frame;
      LOG_DEBUG("allocate frame %p, page num %d", frame, page_num);
      return RC::SUCCESS;
    }

    // 如果所有帧都被分配，则尝试通过 purge 操作释放一些帧
    LOG_TRACE("frames are all allocated, so we should purge some frames to get one free frame");
    (void)frame_manager_.purge_frames(1 /*count*/, purger);
  }
  // 如果无法分配帧，则返回错误代码
  return RC::BUFFERPOOL_NOBUF;
}

/**
 * 检查给定的页号是否有效
 * 
 * 此函数旨在确保提供的页号在文件中是有效的，包括两个步骤：
 * 1. 检查页号是否小于文件的总页数
 * 2. 检查页号在位图中是否被标记为已使用
 * 
 * @param page_num 需要检查的页号
 * @return 如果页号有效，则返回RC::SUCCESS；
 *         如果页号无效，则返回RC::BUFFERPOOL_INVALID_PAGE_NUM
 */
RC DiskBufferPool::check_page_num(PageNum page_num)
{
  // 检查页号是否超出文件的总页数
  if (page_num >= file_header_->page_count) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_name_.c_str());
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }

  // 检查位图中页号对应的位是否被设置为已使用
  if ((file_header_->bitmap[page_num / 8] & (1 << (page_num % 8))) == 0) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_name_.c_str());
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }

  // 页号有效，返回成功
  return RC::SUCCESS;
}

/**
 * 从磁盘加载指定页号的页面到指定的帧中。
 * 
 * 此函数首先尝试通过双重写入机制（dblwr）读取页面。
 * 如果dblwr读取成功，则直接返回成功。
 * 如果dblwr读取失败，则加锁后尝试从磁盘文件中读取页面。
 * 
 * @param page_num 页号，表示要加载的页面在磁盘上的位置。
 * @param frame 指向内存中的帧，用于存储加载的页面数据。
 * @return RC 表示操作的返回状态，可能的值包括：
 *         - RC::SUCCESS: 成功加载页面。
 *         - RC::IOERR_SEEK: lseek操作失败。
 *         - RC::IOERR_READ: 读取操作失败。
 */
RC DiskBufferPool::load_page(PageNum page_num, Frame *frame)
{
  // 获取帧关联的页面引用。
  Page &page = frame->page();
  
  // 尝试通过双重写入管理器读取页面。
  RC rc = dblwr_manager_.read_page(this, page_num, page);
  
  // 如果dblwr读取成功，直接返回成功。
  if (OB_SUCC(rc)) {
    return rc;
  }

  // 加锁以保护磁盘I/O操作。
  scoped_lock lock_guard(wr_lock_);
  
  // 计算页面在文件中的偏移量。
  int64_t          offset = ((int64_t)page_num) * BP_PAGE_SIZE;
  
  // 将文件描述符定位到页面的偏移量。
  if (lseek(file_desc_, offset, SEEK_SET) == -1) {
    // 如果lseek失败，记录错误日志并返回IOERR_SEEK错误。
    LOG_ERROR("Failed to load page %s:%d, due to failed to lseek:%s.", file_name_.c_str(), page_num, strerror(errno));

    return RC::IOERR_SEEK;
  }

  // 从文件中读取页面数据。
  int ret = readn(file_desc_, &page, BP_PAGE_SIZE);
  
  // 如果读取失败，记录错误日志并返回IOERR_READ错误。
  if (ret != 0) {
    LOG_ERROR("Failed to load page %s, file_desc:%d, page num:%d, due to failed to read data:%s, ret=%d, page count=%d",
              file_name_.c_str(), file_desc_, page_num, strerror(errno), ret, file_header_->allocated_pages);
    return RC::IOERR_READ;
  }

  // 设置帧的页号。
  frame->set_page_num(page_num);

  // 记录加载页面的成功日志。
  LOG_DEBUG("Load page %s:%d, file_desc:%d, frame=%s",
            file_name_.c_str(), page_num, file_desc_, frame->to_string().c_str());
  
  // 返回成功。
  return RC::SUCCESS;
}

int DiskBufferPool::file_desc() const { return file_desc_; }

////////////////////////////////////////////////////////////////////////////////
// 构造函数：初始化BufferPoolManager
// 参数 memory_size: 内存池的大小，如果未指定或小于等于0，则使用默认大小
BufferPoolManager::BufferPoolManager(int memory_size /* = 0 */)
{
  // 如果传入的内存池大小小于等于0，则使用默认的内存池大小
  if (memory_size <= 0) {
    memory_size = MEM_POOL_ITEM_NUM * DEFAULT_ITEM_NUM_PER_POOL * BP_PAGE_SIZE;
  }
  
  // 根据内存大小计算池的数量，每个池包含DEFAULT_ITEM_NUM_PER_POOL个页面，每个页面大小为BP_PAGE_SIZE
  const int pool_num = max(memory_size / BP_PAGE_SIZE / DEFAULT_ITEM_NUM_PER_POOL, 1);
  
  // 初始化帧管理器，传入计算出的池数量
  frame_manager_.init(pool_num);
  
  // 日志输出：记录内存池的初始化信息，包括内存大小、页面数量和池数量
  LOG_INFO("buffer pool manager init with memory size %d, page num: %d, pool num: %d",
           memory_size, pool_num * DEFAULT_ITEM_NUM_PER_POOL, pool_num);
}

BufferPoolManager::~BufferPoolManager()
{
  unordered_map<string, DiskBufferPool *> tmp_bps;
  tmp_bps.swap(buffer_pools_);

  for (auto &iter : tmp_bps) {
    delete iter.second;
  }
}

RC BufferPoolManager::init(unique_ptr<DoubleWriteBuffer> dblwr_buffer)
{
  dblwr_buffer_ = std::move(dblwr_buffer);
  return RC::SUCCESS;
}

/**
 * Creates a new file in the buffer pool.
 * 
 * This function is responsible for creating a new file, initializing its basic information, and writing the initial page.
 * It first tries to create and open the file with read-write permissions. If successful, it initializes the file header and writes it to the file.
 * 
 * @param file_name The name of the file to create.
 * @return Returns the result of the creation operation, indicating success or the specific error encountered.
 */
RC BufferPoolManager::create_file(const char *file_name)
{
  // Try to create and open the file with read-write permissions
  int fd = open(file_name, O_RDWR | O_CREAT | O_EXCL, S_IREAD | S_IWRITE);
  if (fd < 0) {
    LOG_ERROR("Failed to create %s, due to %s.", file_name, strerror(errno));
    return RC::SCHEMA_DB_EXIST;
  }

  close(fd);

  /**
   * Here don't care about the failure
   * Reopen the file for read-write, ignoring failures this time
   */
  fd = open(file_name, O_RDWR);
  if (fd < 0) {
    LOG_ERROR("Failed to open for readwrite %s, due to %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }

  // Initialize the first page of the file, including the file header and bitmap
  Page page;
  memset(&page, 0, BP_PAGE_SIZE);

  BPFileHeader *file_header    = (BPFileHeader *)page.data;
  file_header->allocated_pages = 1;
  file_header->page_count      = 1;
  file_header->buffer_pool_id  = next_buffer_pool_id_.fetch_add(1);

  char *bitmap = file_header->bitmap;
  bitmap[0] |= 0x01;

  // Move the file pointer to the beginning of the file
  if (lseek(fd, 0, SEEK_SET) == -1) {
    LOG_ERROR("Failed to seek file %s to position 0, due to %s .", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_SEEK;
  }

  // Write the initialized page data to the file
  if (writen(fd, (char *)&page, BP_PAGE_SIZE) != 0) {
    LOG_ERROR("Failed to write header to file %s, due to %s.", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_WRITE;
  }

  close(fd);
  LOG_INFO("Successfully create %s.", file_name);
  return RC::SUCCESS;
}

/**
 * 打开数据库文件，并在内存中创建对应的缓冲池
 * 
 * @param log_handler 日志处理器，用于记录日志信息
 * @param _file_name 数据库文件名
 * @param _bp 输出参数，指向创建的缓冲池对象
 * @return 返回码，表示操作成功或失败
 * 
 * 此函数首先检查是否有同名文件已经打开，如果有，则返回BUFFERPOOL_OPEN错误码
 * 如果没有，则创建一个新的DiskBufferPool对象，并尝试打开指定的数据库文件
 * 如果文件打开成功，将缓冲池对象与文件名关联，并更新相关的映射表
 */
RC BufferPoolManager::open_file(LogHandler &log_handler, const char *_file_name, DiskBufferPool *&_bp)
{
  // 将文件名转换为字符串，便于后续使用
  string file_name(_file_name);

  // 加锁，保护缓冲池管理器的共享资源
  scoped_lock lock_guard(lock_);
  
  // 检查是否有同名文件已经打开
  if (buffer_pools_.find(file_name) != buffer_pools_.end()) {
    LOG_WARN("file already opened. file name=%s", _file_name);
    return RC::BUFFERPOOL_OPEN;
  }

  // 创建一个新的DiskBufferPool对象
  DiskBufferPool *bp = new DiskBufferPool(*this, frame_manager_, *dblwr_buffer_, log_handler);
  
  // 尝试打开指定的数据库文件
  RC              rc = bp->open_file(_file_name);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open file name");
    delete bp;
    return rc;
  }

  // 确保缓冲池ID的唯一性
  if (bp->id() >= next_buffer_pool_id_.load()) {
    next_buffer_pool_id_.store(bp->id() + 1);
  }

  // 将缓冲池对象与文件名关联，并更新相关的映射表
  buffer_pools_.insert(pair<string, DiskBufferPool *>(file_name, bp));
  id_to_buffer_pools_.insert(pair<int32_t, DiskBufferPool *>(bp->id(), bp));
  
  // 记录调试信息
  LOG_DEBUG("insert buffer pool into fd buffer pools. fd=%d, bp=%p, lbt=%s", bp->file_desc(), bp, lbt());
  
  // 设置输出参数
  _bp = bp;
  
  // 操作成功
  return RC::SUCCESS;
}

/**
 * 关闭一个文件及其对应的缓冲池
 * 
 * 本函数通过文件名找到对应的缓冲池，然后从缓冲池管理器中移除该缓冲池，并释放相关资源
 * 
 * @param _file_name 要关闭的文件名
 * @return RC 错误码，表示操作是否成功
 */
RC BufferPoolManager::close_file(const char *_file_name)
{
  // 将文件名转换为字符串，以便在映射中使用
  string file_name(_file_name);

  // 加锁，以确保线程安全
  lock_.lock();

  // 尝试在缓冲池映射中找到对应的文件名
  auto iter = buffer_pools_.find(file_name);
  // 如果找不到，记录日志并返回INTERNAL错误码
  if (iter == buffer_pools_.end()) {
    LOG_TRACE("file has not opened: %s", _file_name);
    lock_.unlock();
    return RC::INTERNAL;
  }

  // 从ID到缓冲池的映射中移除该项
  id_to_buffer_pools_.erase(iter->second->id());

  // 获取缓冲池指针，然后从映射中移除
  DiskBufferPool *bp = iter->second;
  buffer_pools_.erase(iter);
  // 释放锁，以确保线程安全
  lock_.unlock();

  // 释放缓冲池对象
  delete bp;
  // 操作成功，返回SUCCESS错误码
  return RC::SUCCESS;
}

/**
 * 将指定的页面从缓冲池刷新到磁盘上。
 * 
 * 此函数负责将一个帧（页面）的数据从缓冲池刷新到磁盘上。它首先根据帧所属的缓冲池ID找到对应的缓冲池，
 * 然后调用该缓冲池的flush_page方法来执行实际的刷新操作。
 * 
 * @param frame 待刷新的帧（页面）。
 * @return 返回刷新操作的结果，如果成功则返回RC::SUCCESS，否则返回相应的错误码。
 */
RC BufferPoolManager::flush_page(Frame &frame)
{
  // 获取帧所属的缓冲池ID。
  int buffer_pool_id = frame.buffer_pool_id();

  // 使用锁来确保线程安全。
  scoped_lock lock_guard(lock_);
  
  // 尝试在映射中找到指定ID的缓冲池。
  auto iter = id_to_buffer_pools_.find(buffer_pool_id);
  
  // 如果找不到对应的缓冲池，则记录警告日志并返回内部错误。
  if (iter == id_to_buffer_pools_.end()) {
    LOG_WARN("unknown buffer pool of id %d", buffer_pool_id);
    return RC::INTERNAL;
  }

  // 获取找到的缓冲池指针。
  DiskBufferPool *bp = iter->second;
  
  // 调用缓冲池的flush_page方法来刷新页面，并返回操作结果。
  return bp->flush_page(frame);
}

/**
 * 根据ID获取缓冲池
 * 
 * @param id 缓冲池的标识符
 * @param bp 指向DiskBufferPool对象的指针引用，用于存储找到的缓冲池
 * @return RC::INTERNAL 如果找不到指定ID的缓冲池，则返回内部错误
 * @return RC::SUCCESS 如果成功找到缓冲池，则返回成功
 * 
 * 此函数首先将bp参数设置为nullptr，然后在保护下通过id查找对应的缓冲池
 * 如果找到，则将bp设置为找到的缓冲池指针，并返回成功；否则，返回内部错误
 */
RC BufferPoolManager::get_buffer_pool(int32_t id, DiskBufferPool *&bp)
{
  // 初始化bp为nullptr
  bp = nullptr;

  // 使用scoped_lock进行线程安全保护
  scoped_lock lock_guard(lock_);

  // 在id_to_buffer_pools_映射中查找指定id的缓冲池
  auto iter = id_to_buffer_pools_.find(id);
  // 如果id不存在于映射中，则记录警告日志并返回内部错误
  if (iter == id_to_buffer_pools_.end()) {
    LOG_WARN("unknown buffer pool of id %d", id);
    return RC::INTERNAL;
  }
  
  // 将找到的缓冲池指针赋值给bp，并返回成功
  bp = iter->second;
  return RC::SUCCESS;
}

