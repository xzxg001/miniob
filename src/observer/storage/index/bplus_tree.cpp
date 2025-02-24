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
// Created by Xie Meiyi
// Rewritten by Longda & Wangyunlai
//

#include <span>

#include "storage/index/bplus_tree.h"
#include "common/lang/lower_bound.h"
#include "common/log/log.h"
#include "common/global_context.h"
#include "sql/parser/parse_defs.h"
#include "storage/buffer/disk_buffer_pool.h"

using namespace common;

/**
 * @brief B+树的第一个页面存放的位置
 * @details B+树数据放到Buffer Pool中，Buffer Pool把文件按照固定大小的页面拆分。
 * 第一个页面存放元数据。
 */
#define FIRST_INDEX_PAGE 1

int calc_internal_page_capacity(int attr_length) {
  int item_size = attr_length + sizeof(RID) + sizeof(PageNum); // 计算每个条目的大小
  int capacity  = ((int)BP_PAGE_DATA_SIZE - InternalIndexNode::HEADER_SIZE) / item_size; // 计算内部页的容量
  return capacity;
}

int calc_leaf_page_capacity(int attr_length) {
  int item_size = attr_length + sizeof(RID) + sizeof(RID); // 计算每个条目的大小
  int capacity  = ((int)BP_PAGE_DATA_SIZE - LeafIndexNode::HEADER_SIZE) / item_size; // 计算叶子页的容量
  return capacity;
}

/////////////////////////////////////////////////////////////////////////////////
IndexNodeHandler::IndexNodeHandler(BplusTreeMiniTransaction &mtr, const IndexFileHeader &header, Frame *frame)
    : mtr_(mtr), header_(header), frame_(frame), node_((IndexNode *)frame->data()) // 初始化构造函数
{}

bool IndexNodeHandler::is_leaf() const { return node_->is_leaf; } // 判断节点是否为叶子节点

void IndexNodeHandler::init_empty(bool leaf) {
  node_->is_leaf = leaf; // 设置节点类型
  node_->key_num = 0; // 初始化键数量
  node_->parent  = BP_INVALID_PAGE_NUM; // 设置父节点为无效
}

PageNum IndexNodeHandler::page_num() const { return frame_->page_num(); } // 获取页码

int IndexNodeHandler::key_size() const { return header_.key_length; } // 获取键的大小

int IndexNodeHandler::value_size() const {
  // return header_.value_size;
  return sizeof(RID); // 获取值的大小
}

int IndexNodeHandler::item_size() const { return key_size() + value_size(); } // 获取条目大小

int IndexNodeHandler::size() const { return node_->key_num; } // 获取当前节点的键数量

int IndexNodeHandler::max_size() const { return is_leaf() ? header_.leaf_max_size : header_.internal_max_size; } // 获取最大大小

int IndexNodeHandler::min_size() const {
  const int max = this->max_size(); // 获取最大容量
  return max - max / 2; // 返回最小容量
}

void IndexNodeHandler::increase_size(int n) {
  node_->key_num += n; // 增加键数量
}

PageNum IndexNodeHandler::parent_page_num() const { return node_->parent; } // 获取父节点页码

RC IndexNodeHandler::set_parent_page_num(PageNum page_num) {
  RC rc = mtr_.logger().set_parent_page(*this, page_num, this->node_->parent); // 记录父节点变化
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log set parent page. rc=%s", strrc(rc));
    return rc;
  }
  this->node_->parent = page_num; // 更新父节点页码
  return rc;
}

/**
 * 检查一个节点经过插入或删除操作后是否需要分裂或合并操作
 * @return true 需要分裂或合并；
 *         false 不需要分裂或合并
 */
bool IndexNodeHandler::is_safe(BplusTreeOperationType op, bool is_root_node) {
  switch (op) {
    case BplusTreeOperationType::READ: {
      return true; // 读取操作始终安全
    } break;
    case BplusTreeOperationType::INSERT: {
      return size() < max_size(); // 插入时检查是否超出最大大小
    } break;
    case BplusTreeOperationType::DELETE: {
      if (is_root_node) {  // 根节点特殊处理
        if (node_->is_leaf) {
          return size() > 1;  // 叶子节点的根必须保留至少一个条目
        }
        // 非叶子节点
        // 如果删除一个子节点后只剩下一个子节点，则需要删除自己
        return size() > 2; 
      }
      return size() > min_size(); // 非根节点，检查最小大小
    } break;
    default: {
      // 不处理其他情况
    } break;
  }

  ASSERT(false, "invalid operation type: %d", static_cast<int>(op)); // 检查操作类型有效性
  return false;
}

string to_string(const IndexNodeHandler &handler) {
  stringstream ss;

  ss << "PageNum:" << handler.page_num() << ",is_leaf:" << handler.is_leaf() << ","
     << "key_num:" << handler.size() << ","
     << "parent:" << handler.parent_page_num() << ",";

  return ss.str(); // 返回节点信息字符串
}

bool IndexNodeHandler::validate() const {
  if (parent_page_num() == BP_INVALID_PAGE_NUM) { // 检查是否为根节点
    // 这是一个根页
    if (size() < 1) {
      LOG_WARN("root page has no item"); // 根页没有条目
      return false;
    }

    if (!is_leaf() && size() < 2) {
      LOG_WARN("root page internal node has less than 2 child. size=%d", size()); // 非叶子根页至少要有两个子节点
      return false;
    }
  }
  return true; // 验证通过
}

RC IndexNodeHandler::recover_insert_items(int index, const char *items, int num) {
  const int item_size = this->item_size(); // 获取条目大小
  if (index < size()) {
    memmove(__item_at(index + num), __item_at(index), (static_cast<size_t>(size()) - index) * item_size); // 移动条目
  }

  memcpy(__item_at(index), items, static_cast<size_t>(num) * item_size); // 插入新条目
  increase_size(num); // 增加条目数量
  return RC::SUCCESS;
}

RC IndexNodeHandler::recover_remove_items(int index, int num) {
  const int item_size = this->item_size(); // 获取条目大小
  if (index < size() - num) {
    memmove(__item_at(index), __item_at(index + num), (static_cast<size_t>(size()) - index - num) * item_size); // 移动条目
  }

  increase_size(-num); // 减少条目数量
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
LeafIndexNodeHandler::LeafIndexNodeHandler(BplusTreeMiniTransaction &mtr, const IndexFileHeader &header, Frame *frame)
    : IndexNodeHandler(mtr, header, frame), leaf_node_((LeafIndexNode *)frame->data()) // 初始化构造函数
{}

RC LeafIndexNodeHandler::init_empty() {
  RC rc = mtr_.logger().leaf_init_empty(*this); // 记录初始化操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log init empty leaf node. rc=%s", strrc(rc));
    return rc;
  }
  IndexNodeHandler::init_empty(true/*leaf*/); // 初始化为空叶子节点
  leaf_node_->next_brother = BP_INVALID_PAGE_NUM; // 设置下一个兄弟节点为无效
  return RC::SUCCESS;
}

RC LeafIndexNodeHandler::set_next_page(PageNum page_num) {
  RC rc = mtr_.logger().leaf_set_next_page(*this, page_num, leaf_node_->next_brother); // 记录下一个页面
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log set next page. rc=%s", strrc(rc));
    return rc;
  }

  leaf_node_->next_brother = page_num; // 更新下一个页面
  return RC::SUCCESS;
}

PageNum LeafIndexNodeHandler::next_page() const { return leaf_node_->next_brother; } // 获取下一个页面的页码

char *LeafIndexNodeHandler::key_at(int index) {
  assert(index >= 0 && index < size()); // 检查索引有效性
  return __key_at(index); // 返回指定索引的键
}

char *LeafIndexNodeHandler::value_at(int index) {
  assert(index >= 0 && index < size()); // 检查索引有效性
  return __value_at(index); // 返回指定索引的值
}

int LeafIndexNodeHandler::lookup(const KeyComparator &comparator, const char *key, bool *found /* = nullptr */) const
{
  const int size = this->size(); // 获取当前节点的条目数量
  common::BinaryIterator<char> iter_begin(item_size(), __key_at(0)); // 创建迭代器，指向第一个条目
  common::BinaryIterator<char> iter_end(item_size(), __key_at(size)); // 创建迭代器，指向最后一个条目
  common::BinaryIterator<char> iter = lower_bound(iter_begin, iter_end, key, comparator, found); // 查找键的位置
  return iter - iter_begin; // 返回键的位置索引
}

RC LeafIndexNodeHandler::insert(int index, const char *key, const char *value)
{
  vector<char> item(key_size() + value_size()); // 创建一个大小为键+值的vector
  memcpy(item.data(), key, key_size()); // 复制键到item中
  memcpy(item.data() + key_size(), value, value_size()); // 复制值到item中

  RC rc = mtr_.logger().node_insert_items(*this, index, item, 1); // 记录插入操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log insert item. rc=%s", strrc(rc));
    return rc;
  }

  recover_insert_items(index, item.data(), 1); // 更新当前节点的条目
  return RC::SUCCESS;
}

RC LeafIndexNodeHandler::remove(int index)
{
  assert(index >= 0 && index < size()); // 检查索引有效性

  RC rc = mtr_.logger().node_remove_items(*this, index, span<const char>(__item_at(index), item_size()), 1); // 记录删除操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log remove item. rc=%s", strrc(rc));
    return rc;
  }

  recover_remove_items(index, 1); // 更新当前节点的条目
  return RC::SUCCESS;
}

int LeafIndexNodeHandler::remove(const char *key, const KeyComparator &comparator)
{
  bool found = false;
  int index = lookup(comparator, key, &found); // 查找键的位置
  if (found) {
    this->remove(index); // 如果找到，删除该键
    return 1;
  }
  return 0; // 未找到，返回0
}

RC LeafIndexNodeHandler::move_half_to(LeafIndexNodeHandler &other)
{
  const int size = this->size(); // 获取当前节点的条目数量
  const int move_index = size / 2; // 计算移动开始的索引
  const int move_item_num = size - move_index; // 计算移动的条目数量

  other.append(__item_at(move_index), move_item_num); // 将后半部分条目移动到另一个节点

  RC rc = mtr_.logger().node_remove_items(*this, move_index, span<const char>(__item_at(move_index), move_item_num * item_size()), move_item_num); // 记录缩小操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log shrink leaf node. rc=%s", strrc(rc));
    return rc;
  }

  recover_remove_items(move_index, move_item_num); // 更新当前节点的条目
  return RC::SUCCESS;
}

RC LeafIndexNodeHandler::move_first_to_end(LeafIndexNodeHandler &other)
{
  other.append(__item_at(0)); // 将第一个条目移动到另一个节点的末尾

  return this->remove(0); // 从当前节点删除第一个条目
}

RC LeafIndexNodeHandler::move_last_to_front(LeafIndexNodeHandler &other)
{
  other.preappend(__item_at(size() - 1)); // 将最后一个条目移动到另一个节点的开头

  this->remove(size() - 1); // 从当前节点删除最后一个条目
  return RC::SUCCESS;
}

/**
 * @brief 将所有条目移动到左侧页面
 */
RC LeafIndexNodeHandler::move_to(LeafIndexNodeHandler &other)
{
  other.append(__item_at(0), this->size()); // 将当前节点的所有条目添加到另一个节点
  other.set_next_page(this->next_page()); // 设置另一个节点的下一个页面为当前节点的下一个页面

  RC rc = mtr_.logger().node_remove_items(*this, 0, span<const char>(__item_at(0), this->size() * item_size()), this->size()); // 记录缩小操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log shrink leaf node. rc=%s", strrc(rc));
  }
  this->increase_size(-this->size()); // 更新当前节点的条目数

  return RC::SUCCESS;
}

// 将一些数据追加到当前节点的最右边
RC LeafIndexNodeHandler::append(const char *items, int num)
{
  RC rc = mtr_.logger().node_insert_items(*this, size(), span<const char>(items, num * item_size()), num); // 记录追加操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log append items. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  return recover_insert_items(size(), items, num); // 更新当前节点的条目
}

RC LeafIndexNodeHandler::append(const char *item)
{
  return append(item, 1); // 追加单个条目
}

RC LeafIndexNodeHandler::preappend(const char *item)
{
  return insert(0, item, item + key_size()); // 将条目插入到当前节点的开头
}

char *LeafIndexNodeHandler::__item_at(int index) const { return leaf_node_->array + (index * item_size()); } // 获取指定索引的条目

string to_string(const LeafIndexNodeHandler &handler, const KeyPrinter &printer)
{
  stringstream ss;
  ss << to_string((const IndexNodeHandler &)handler) << ",next page:" << handler.next_page(); // 转换为字符串，包含下一页信息
  ss << ",values=[" << printer(handler.__key_at(0)); // 添加第一个键
  for (int i = 1; i < handler.size(); i++) {
    ss << "," << printer(handler.__key_at(i)); // 添加其余的键
  }
  ss << "]";
  return ss.str(); // 返回构造的字符串
}

bool LeafIndexNodeHandler::validate(const KeyComparator &comparator, DiskBufferPool *bp) const
{
  bool result = IndexNodeHandler::validate(); // 验证父类的有效性
  if (false == result) {
    return false; // 父类无效，返回false
  }

  const int node_size = size(); // 获取当前节点大小
  for (int i = 1; i < node_size; i++) {
    if (comparator(__key_at(i - 1), __key_at(i)) >= 0) {
      LOG_WARN("page number = %d, invalid key order. id1=%d,id2=%d, this=%s",
               page_num(), i - 1, i, to_string(*this).c_str());
      return false; // 检查键的顺序是否有效
    }
  }

  PageNum parent_page_num = this->parent_page_num(); // 获取父节点页码
  if (parent_page_num == BP_INVALID_PAGE_NUM) {
    return true; // 如果没有父节点，返回true
  }

  Frame *parent_frame = nullptr;
  RC rc = bp->get_this_page(parent_page_num, &parent_frame); // 获取父节点的页面
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to fetch parent page. page num=%d, rc=%d:%s", parent_page_num, rc, strrc(rc));
    return false;
  }

  InternalIndexNodeHandler parent_node(mtr_, header_, parent_frame); // 创建父节点处理器
  int index_in_parent = parent_node.value_index(this->page_num()); // 获取当前节点在父节点中的索引
  if (index_in_parent < 0) {
    LOG_WARN("invalid leaf node. cannot find index in parent. this page num=%d, parent page num=%d",
             this->page_num(), parent_page_num);
    bp->unpin_page(parent_frame);
    return false; // 如果未找到，返回false
  }

  if (0 != index_in_parent) {
    int cmp_result = comparator(__key_at(0), parent_node.key_at(index_in_parent)); // 比较当前节点的第一个键与父节点中的键
    if (cmp_result < 0) {
      LOG_WARN("invalid leaf node. first item should be greater than or equal to parent item. "
               "this page num=%d, parent page num=%d, index in parent=%d",
               this->page_num(), parent_node.page_num(), index_in_parent);
      bp->unpin_page(parent_frame);
      return false; // 如果当前节点的第一个键小于父节点中的键，返回false
    }
  }

  if (index_in_parent < parent_node.size() - 1) {
    int cmp_result = comparator(__key_at(size() - 1), parent_node.key_at(index_in_parent + 1)); // 比较当前节点的最后一个键与父节点中的下一个键
    if (cmp_result >= 0) {
      LOG_WARN("invalid leaf node. last item should be less than the item at the first after item in parent."
               "this page num=%d, parent page num=%d, parent item to compare=%d",
               this->page_num(), parent_node.page_num(), index_in_parent + 1);
      bp->unpin_page(parent_frame);
      return false; // 如果当前节点的最后一个键大于等于父节点中的下一个键，返回false
    }
  }
  bp->unpin_page(parent_frame); // 解锁父节点页面
  return true; // 验证通过
}

/////////////////////////////////////////////////////////////////////////////////
InternalIndexNodeHandler::InternalIndexNodeHandler(BplusTreeMiniTransaction &mtr, const IndexFileHeader &header, Frame *frame)
    : IndexNodeHandler(mtr, header, frame), internal_node_((InternalIndexNode *)frame->data())
{}

string to_string(const InternalIndexNodeHandler &node, const KeyPrinter &printer)
{
  stringstream ss;
  ss << to_string((const IndexNodeHandler &)node);
  ss << ",children:[" // 开始构建子节点的字符串表示
     << "{key:" << printer(node.__key_at(0)) << "," // 添加第一个子节点的键
     << "value:" << *(PageNum *)node.__value_at(0) << "}";

  for (int i = 1; i < node.size(); i++) {
    ss << ",{key:" << printer(node.__key_at(i)) << ",value:" << *(PageNum *)node.__value_at(i) << "}"; // 添加其余子节点
  }
  ss << "]";
  return ss.str(); // 返回构造的字符串
}

RC InternalIndexNodeHandler::init_empty() 
{
  RC rc = mtr_.logger().internal_init_empty(*this); // 记录初始化空内部节点操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log init empty internal node. rc=%s", strrc(rc));
  }
  IndexNodeHandler::init_empty(false /* leaf */); // 初始化父类
  return RC::SUCCESS;
}

RC InternalIndexNodeHandler::create_new_root(PageNum first_page_num, const char *key, PageNum page_num)
{
  RC rc = mtr_.logger().internal_create_new_root(*this, first_page_num, span<const char>(key, key_size()), page_num); // 记录创建新根节点操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log create new root. rc=%s", strrc(rc));
  }

  memset(__key_at(0), 0, key_size()); // 初始化根节点的第一个键
  memcpy(__value_at(0), &first_page_num, value_size()); // 设置第一个子节点的页码
  memcpy(__item_at(1), key, key_size()); // 设置根节点的第二个键
  memcpy(__value_at(1), &page_num, value_size()); // 设置第二个子节点的页码
  increase_size(2); // 更新节点大小
  return RC::SUCCESS;
}

/**
 * @brief 插入一个条目
 * @details 插入的条目永远不会在第一个槽位。
 * 分裂后的右子节点将始终拥有更大的键。
 * @NOTE 我们在这里不需要设置子节点的父页面编号。你可以在调用者中查看详情。
 * 这也便于单元测试，不设置子节点的父页面编号。
 */
RC InternalIndexNodeHandler::insert(const char *key, PageNum page_num, const KeyComparator &comparator)
{
  int insert_position = -1;
  lookup(comparator, key, nullptr, &insert_position); // 查找插入位置
  vector<char> item(key_size() + sizeof(PageNum)); // 创建一个大小为键+页码的vector
  memcpy(item.data(), key, key_size()); // 复制键到item中
  memcpy(item.data() + key_size(), &page_num, sizeof(PageNum)); // 复制页码到item中
  RC rc = mtr_.logger().node_insert_items(*this, insert_position, span<const char>(item.data(), item.size()), 1); // 记录插入操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log insert item. rc=%s", strrc(rc));
    return rc;
  }

  return recover_insert_items(insert_position, item.data(), 1); // 更新当前节点的条目
}

/**
 * @brief 将一半的条目移动到另一个节点
 */
RC InternalIndexNodeHandler::move_half_to(InternalIndexNodeHandler &other)
{
  const int size = this->size(); // 获取当前节点的条目数量
  const int move_index = size / 2; // 计算移动开始的索引
  const int move_num = size - move_index; // 计算移动的条目数量
  RC rc = other.append(this->__item_at(move_index), size - move_index); // 将后半部分条目移动到另一个节点
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to copy item to new node. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  mtr_.logger().node_remove_items(*this, move_index, span<const char>(__item_at(move_index), move_num * item_size()), move_num); // 记录缩小操作
  increase_size(-(size - move_index)); // 更新当前节点的条目数
  return rc;
}



/**
 * lookup the first item which key <= item
 * @return unlike the leafNode, the return value is not the insert position,
 * but only the index of child to find.
 */
int InternalIndexNodeHandler::lookup(const KeyComparator &comparator, const char *key, bool *found /* = nullptr */,
    int *insert_position /*= nullptr */) const
{
  const int size = this->size();
  if (size == 0) {
    if (insert_position) {
      *insert_position = 1; // 如果没有元素，插入位置为1
    }
    if (found) {
      *found = false; // 没有找到元素
    }
    return 0; // 返回0
  }

  common::BinaryIterator<char> iter_begin(item_size(), __key_at(1)); // 起始迭代器
  common::BinaryIterator<char> iter_end(item_size(), __key_at(size)); // 结束迭代器
  common::BinaryIterator<char> iter = lower_bound(iter_begin, iter_end, key, comparator, found); // 查找位置
  int ret = static_cast<int>(iter - iter_begin) + 1; // 计算返回位置
  if (insert_position) {
    *insert_position = ret; // 更新插入位置
  }

  if (ret >= size || comparator(key, __key_at(ret)) < 0) {
    return ret - 1; // 返回找到的位置或前一个位置
  }
  return ret; // 返回找到的位置
}

char *InternalIndexNodeHandler::key_at(int index)
{
  assert(index >= 0 && index < size()); // 确保索引合法
  return __key_at(index); // 返回指定索引的键
}

void InternalIndexNodeHandler::set_key_at(int index, const char *key)
{
  assert(index >= 0 && index < size()); // 确保索引合法

  mtr_.logger().internal_update_key(*this, index, span<const char>(key, key_size()), span<const char>(__key_at(index), key_size())); // 记录键的更新
  memcpy(__key_at(index), key, key_size()); // 设置新的键
}

PageNum InternalIndexNodeHandler::value_at(int index)
{
  assert(index >= 0 && index < size()); // 确保索引合法
  return *(PageNum *)__value_at(index); // 返回指定索引的值
}

int InternalIndexNodeHandler::value_index(PageNum page_num)
{
  for (int i = 0; i < size(); i++) {
    if (page_num == *(PageNum *)__value_at(i)) {
      return i; // 返回值的索引
    }
  }
  return -1; // 找不到返回-1
}

void InternalIndexNodeHandler::remove(int index)
{
  assert(index >= 0 && index < size()); // 确保索引合法

  BplusTreeLogger &logger = mtr_.logger();
  RC rc = logger.node_remove_items(*this, index, span<const char>(__item_at(index), item_size()), 1); // 记录移除操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log remove item. rc=%s. node=%s", strrc(rc), to_string(*this).c_str()); // 记录警告
  }

  recover_remove_items(index, 1); // 恢复移除的项
}

RC InternalIndexNodeHandler::move_to(InternalIndexNodeHandler &other)
{
  RC rc = other.append(__item_at(0), size()); // 将项移动到其他节点
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to copy items to other node. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  rc = mtr_.logger().node_remove_items(*this, 0, span<const char>(__item_at(0), size() * item_size()), size()); // 记录移除操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log shrink internal node. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  recover_remove_items(0, size()); // 恢复移除的项
  return RC::SUCCESS; // 返回成功
}

RC InternalIndexNodeHandler::move_first_to_end(InternalIndexNodeHandler &other)
{
  RC rc = other.append(__item_at(0)); // 将第一个项移动到其他节点的末尾
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to append item to others."); // 记录警告
    return rc;
  }

  remove(0); // 移除当前节点的第一个项
  return rc; // 返回结果
}

RC InternalIndexNodeHandler::move_last_to_front(InternalIndexNodeHandler &other)
{
  RC rc = other.preappend(__item_at(size() - 1)); // 将最后一个项移动到其他节点的开头
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to preappend to others"); // 记录警告
    return rc;
  }

  rc = mtr_.logger().node_remove_items(*this, size() - 1, span<const char>(__item_at(size() - 1), item_size()), 1); // 记录移除操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log shrink internal node. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  increase_size(-1); // 更新节点大小
  return rc; // 返回结果
}

RC InternalIndexNodeHandler::insert_items(int index, const char *items, int num)
{
  RC rc = mtr_.logger().node_insert_items(*this, index, span<const char>(items, item_size() * num), num); // 记录插入操作
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to log insert item. rc=%s", strrc(rc));
    return rc;
  }

  recover_insert_items(index, items, num); // 恢复插入的项

  LatchMemo &latch_memo = mtr_.latch_memo();
  PageNum this_page_num = this->page_num();
  Frame *frame = nullptr;

  // 设置所有页面的父页面为当前页面
  // 这里会访问大量的页面，可能会将它们从磁盘加载到内存中而占用大量的缓冲池页面
  for (int i = 0; i < num; i++) {
    const PageNum page_num = *(const PageNum *)((items + i * item_size()) + key_size());
    rc = latch_memo.get_page(page_num, frame); // 获取子页面
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to set child's page num. child page num:%d, this page num=%d, rc=%d:%s",
               page_num, this_page_num, rc, strrc(rc));
      return rc;
    }
    IndexNodeHandler child_node(mtr_, header_, frame);
    child_node.set_parent_page_num(this_page_num); // 设置子页面的父页面
    frame->mark_dirty(); // 标记页面为脏
  }

  return RC::SUCCESS; // 返回成功
}

/**
 * 将其他节点的项复制到当前节点的右侧
 */
RC InternalIndexNodeHandler::append(const char *items, int num)
{
  return insert_items(size(), items, num); // 在末尾插入项
}

RC InternalIndexNodeHandler::append(const char *item) { return this->append(item, 1); } // 单个项插入末尾

RC InternalIndexNodeHandler::preappend(const char *item)
{
  return this->insert_items(0, item, 1); // 单个项插入开头
}

char *InternalIndexNodeHandler::__item_at(int index) const { return internal_node_->array + (index * item_size()); } // 返回指定索引的项

int InternalIndexNodeHandler::value_size() const { return sizeof(PageNum); } // 返回值的大小

int InternalIndexNodeHandler::item_size() const { return key_size() + this->value_size(); } // 返回项的大小

bool InternalIndexNodeHandler::validate(const KeyComparator &comparator, DiskBufferPool *bp) const
{
  bool result = IndexNodeHandler::validate(); // 验证父类的有效性
  if (false == result) {
    return false; // 如果无效，返回false
  }

  const int node_size = size();
  for (int i = 2; i < node_size; i++) {
    if (comparator(__key_at(i - 1), __key_at(i)) >= 0) {
      LOG_WARN("page number = %d, invalid key order. id1=%d,id2=%d, this=%s",
          page_num(), i - 1, i, to_string(*this).c_str()); // 记录无效键顺序的警告
      return false; // 返回无效
    }
  }

  for (int i = 0; result && i < node_size; i++) {
    PageNum page_num = *(PageNum *)__value_at(i);
    if (page_num < 0) {
      LOG_WARN("this page num=%d, got invalid child page. page num=%d", this->page_num(), page_num); // 记录无效子页面警告
    } else {
      Frame *child_frame = nullptr;
      RC rc = bp->get_this_page(page_num, &child_frame); // 获取子页面
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to fetch child page while validate internal page. page num=%d, rc=%d:%s", 
                 page_num, rc, strrc(rc)); // 记录获取子页面失败的警告
      } else {
        IndexNodeHandler child_node(mtr_, header_, child_frame);
        if (child_node.parent_page_num() != this->page_num()) {
          LOG_WARN("child's parent page num is invalid. child page num=%d, parent page num=%d, this page num=%d",
              child_node.page_num(), child_node.parent_page_num(), this->page_num()); // 记录子页面的父页面无效的警告
          result = false; // 设置结果为无效
        }
        bp->unpin_page(child_frame); // 取消固定子页面
      }
    }
  }

  if (!result) {
    return result; // 如果无效，返回结果
  }

  const PageNum parent_page_num = this->parent_page_num();
  if (parent_page_num == BP_INVALID_PAGE_NUM) {
    return result; // 如果父页面无效，返回结果
  }

  Frame *parent_frame = nullptr;
  RC rc = bp->get_this_page(parent_page_num, &parent_frame); // 获取父页面
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to fetch parent page. page num=%d, rc=%d:%s", parent_page_num, rc, strrc(rc));
    return false; // 返回无效
  }

  InternalIndexNodeHandler parent_node(mtr_, header_, parent_frame);

  int index_in_parent = parent_node.value_index(this->page_num()); // 获取在父页面的索引
  if (index_in_parent < 0) {
    LOG_WARN("invalid internal node. cannot find index in parent. this page num=%d, parent page num=%d",
             this->page_num(), parent_page_num); // 记录在父页面找不到索引的警告
    bp->unpin_page(parent_frame); // 取消固定父页面
    return false; // 返回无效
  }

  if (0 != index_in_parent) {
    int cmp_result = comparator(__key_at(1), parent_node.key_at(index_in_parent)); // 比较键
    if (cmp_result < 0) {
      LOG_WARN("invalid internal node. the second item should be greater than or equal to parent item. "
               "this page num=%d, parent page num=%d, index in parent=%d",
               this->page_num(), parent_node.page_num(), index_in_parent); // 记录第二个项小于父项的警告
      bp->unpin_page(parent_frame); // 取消固定父页面
      return false; // 返回无效
    }
  }

  if (index_in_parent < parent_node.size() - 1) {
    int cmp_result = comparator(__key_at(size() - 1), parent_node.key_at(index_in_parent + 1)); // 比较最后一个键
    if (cmp_result >= 0) {
      LOG_WARN("invalid internal node. last item should be less than the item at the first after item in parent."
               "this page num=%d, parent page num=%d, parent item to compare=%d",
               this->page_num(), parent_node.page_num(), index_in_parent + 1); // 记录最后一个项不符合条件的警告
      bp->unpin_page(parent_frame); // 取消固定父页面
      return false; // 返回无效
    }
  }
  bp->unpin_page(parent_frame); // 取消固定父页面

  return result; // 返回结果
}


/////////////////////////////////////////////////////////////////////////////////
RC BplusTreeHandler::sync()
{
  if (header_dirty_) { // 如果头部已被修改
    Frame *frame = nullptr;

    // 获取索引的第一页面
    RC rc = disk_buffer_pool_->get_this_page(FIRST_INDEX_PAGE, &frame);
    if (OB_SUCC(rc) && frame != nullptr) {
      char *pdata = frame->data(); // 获取页面数据
      memcpy(pdata, &file_header_, sizeof(file_header_)); // 复制文件头信息
      frame->mark_dirty(); // 标记页面为脏
      disk_buffer_pool_->unpin_page(frame); // 释放页面
      header_dirty_ = false; // 重置头部脏标志
    } else {
      LOG_WARN("failed to sync index header file. file_desc=%d, rc=%s", disk_buffer_pool_->file_desc(), strrc(rc)); // 记录警告
      // TODO: 忽略?
    }
  }
  return disk_buffer_pool_->flush_all_pages(); // 刷新所有页面
}

RC BplusTreeHandler::create(LogHandler &log_handler,
                            BufferPoolManager &bpm,
                            const char *file_name, 
                            AttrType attr_type, 
                            int attr_length, 
                            int internal_max_size /* = -1*/,
                            int leaf_max_size /* = -1 */)
{
  RC rc = bpm.create_file(file_name); // 创建文件
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to create file. file name=%s, rc=%d:%s", file_name, rc, strrc(rc)); // 记录警告
    return rc;
  }
  LOG_INFO("Successfully create index file:%s", file_name); // 记录成功信息

  DiskBufferPool *bp = nullptr;

  rc = bpm.open_file(log_handler, file_name, bp); // 打开文件
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to open file. file name=%s, rc=%d:%s", file_name, rc, strrc(rc)); // 记录警告
    return rc;
  }
  LOG_INFO("Successfully open index file %s.", file_name); // 记录成功信息

  rc = this->create(log_handler, *bp, attr_type, attr_length, internal_max_size, leaf_max_size); // 创建B+树
  if (OB_FAIL(rc)) {
    bpm.close_file(file_name); // 关闭文件
    return rc;
  }

  LOG_INFO("Successfully create index file %s.", file_name); // 记录成功信息
  return rc; // 返回结果
}

RC BplusTreeHandler::create(LogHandler &log_handler,
            DiskBufferPool &buffer_pool,
            AttrType attr_type,
            int attr_length,
            int internal_max_size /* = -1 */,
            int leaf_max_size /* = -1 */)
{
  if (internal_max_size < 0) {
    internal_max_size = calc_internal_page_capacity(attr_length); // 计算内部页面容量
  }
  if (leaf_max_size < 0) {
    leaf_max_size = calc_leaf_page_capacity(attr_length); // 计算叶子页面容量
  }

  log_handler_      = &log_handler; // 设置日志处理器
  disk_buffer_pool_ = &buffer_pool; // 设置磁盘缓冲池

  RC rc = RC::SUCCESS;

  BplusTreeMiniTransaction mtr(*this, &rc); // 创建B+树小事务

  Frame *header_frame = nullptr;

  rc = mtr.latch_memo().allocate_page(header_frame); // 分配头部页面
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to allocate header page for bplus tree. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc;
  }

  if (header_frame->page_num() != FIRST_INDEX_PAGE) {
    LOG_WARN("header page num should be %d but got %d. is it a new file",
             FIRST_INDEX_PAGE, header_frame->page_num()); // 检查头部页面编号
    return RC::INTERNAL; // 返回内部错误
  }

  char            *pdata         = header_frame->data(); // 获取页面数据
  IndexFileHeader *file_header   = (IndexFileHeader *)pdata; // 指向文件头
  file_header->attr_length       = attr_length; // 设置属性长度
  file_header->key_length        = attr_length + sizeof(RID); // 设置键长度
  file_header->attr_type         = attr_type; // 设置属性类型
  file_header->internal_max_size = internal_max_size; // 设置内部最大容量
  file_header->leaf_max_size     = leaf_max_size; // 设置叶子最大容量
  file_header->root_page         = BP_INVALID_PAGE_NUM; // 设置根页面为无效

  // 取消记录日志的原因请参考下面的sync调用的地方。
  // mtr.logger().init_header_page(header_frame, *file_header); // 初始化日志记录

  header_frame->mark_dirty(); // 标记头部页面为脏

  memcpy(&file_header_, pdata, sizeof(file_header_)); // 复制文件头信息
  header_dirty_ = false; // 重置头部脏标志

  mem_pool_item_ = make_unique<common::MemPoolItem>("b+tree"); // 创建内存池项目
  if (mem_pool_item_->init(file_header->key_length) < 0) { // 初始化内存池
    LOG_WARN("Failed to init memory pool for index"); // 记录警告
    close(); // 关闭
    return RC::NOMEM; // 返回内存不足错误
  }

  key_comparator_.init(file_header->attr_type, file_header->attr_length); // 初始化键比较器
  key_printer_.init(file_header->attr_type, file_header->attr_length); // 初始化键打印器

  // 虽然我们针对B+树记录了WAL，但我们记录的都是逻辑日志，并没有记录某个页面如何修改的物理日志。
  // 在做恢复时，必须先创建出来一个tree handler对象。但是如果元数据页面不正确的话，我们无法创建一个正确的tree handler对象。
  // 因此这里取消第一次元数据页面修改的WAL记录，而改用更简单的方式，直接将元数据页面刷到磁盘。
  rc = this->sync(); // 同步数据
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to sync index header. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc;
  }

  LOG_INFO("Successfully create index"); // 记录成功信息
  return RC::SUCCESS; // 返回成功
}

RC BplusTreeHandler::open(LogHandler &log_handler, BufferPoolManager &bpm, const char *file_name)
{
  if (disk_buffer_pool_ != nullptr) { // 如果已经打开
    LOG_WARN("%s has been opened before index.open.", file_name); // 记录警告
    return RC::RECORD_OPENNED; // 返回已打开错误
  }

  DiskBufferPool *disk_buffer_pool = nullptr;

  RC rc = bpm.open_file(log_handler, file_name, disk_buffer_pool); // 打开文件
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to open file name=%s, rc=%d:%s", file_name, rc, strrc(rc)); // 记录警告
    return rc;
  }

  rc = this->open(log_handler, *disk_buffer_pool); // 打开B+树
  if (OB_SUCC(rc)) {
    LOG_INFO("open b+tree success. filename=%s", file_name); // 记录成功信息
  }
  return rc; // 返回结果
}

RC BplusTreeHandler::open(LogHandler &log_handler, DiskBufferPool &buffer_pool)
{
  if (disk_buffer_pool_ != nullptr) { // 如果已经打开
    LOG_WARN("b+tree has been opened before index.open."); // 记录警告
    return RC::RECORD_OPENNED; // 返回已打开错误
  }

  RC rc = RC::SUCCESS;

  Frame *frame = nullptr;
  rc           = buffer_pool.get_this_page(FIRST_INDEX_PAGE, &frame); // 获取第一页面
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to get first page, rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc;
  }

  char *pdata = frame->data(); // 获取页面数据
  memcpy(&file_header_, pdata, sizeof(IndexFileHeader)); // 复制文件头信息
  header_dirty_     = false; // 重置头部脏标志
  disk_buffer_pool_ = &buffer_pool; // 设置磁盘缓冲池
  log_handler_      = &log_handler; // 设置日志处理器

  mem_pool_item_ = make_unique<common::MemPoolItem>("b+tree"); // 创建内存池项目
  if (mem_pool_item_->init(file_header_.key_length) < 0) { // 初始化内存池
    LOG_WARN("Failed to init memory pool for index"); // 记录警告
    close(); // 关闭
    return RC::NOMEM; // 返回内存不足错误
  }

  // 取消固定页面
  buffer_pool.unpin_page(frame);

  key_comparator_.init(file_header_.attr_type, file_header_.attr_length); // 初始化键比较器
  key_printer_.init(file_header_.attr_type, file_header_.attr_length); // 初始化键打印器
  LOG_INFO("Successfully open index"); // 记录成功信息
  return RC::SUCCESS; // 返回成功
}

RC BplusTreeHandler::close()
{
  if (disk_buffer_pool_ != nullptr) {
    disk_buffer_pool_->close_file(); // 关闭文件
  }

  disk_buffer_pool_ = nullptr; // 重置磁盘缓冲池
  return RC::SUCCESS; // 返回成功
}

RC BplusTreeHandler::print_leaf(Frame *frame)
{
  BplusTreeMiniTransaction mtr(*this); // 创建小事务
  LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 处理叶子节点
  LOG_INFO("leaf node: %s", to_string(leaf_node, key_printer_).c_str()); // 打印叶子节点信息
  disk_buffer_pool_->unpin_page(frame); // 取消固定页面
  return RC::SUCCESS; // 返回成功
}

RC BplusTreeHandler::print_internal_node_recursive(Frame *frame)
{
  RC rc = RC::SUCCESS;
  BplusTreeMiniTransaction mtr(*this); // 创建小事务

  LOG_INFO("bplus tree. file header: %s", file_header_.to_string().c_str()); // 打印文件头信息
  InternalIndexNodeHandler internal_node(mtr, file_header_, frame); // 处理内部节点
  LOG_INFO("internal node: %s", to_string(internal_node, key_printer_).c_str()); // 打印内部节点信息

  int node_size = internal_node.size(); // 获取节点大小
  for (int i = 0; i < node_size; i++) {
    PageNum page_num = internal_node.value_at(i); // 获取子页面编号
    Frame  *child_frame;
    rc = disk_buffer_pool_->get_this_page(page_num, &child_frame); // 获取子页面
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to fetch child page. page id=%d, rc=%d:%s", page_num, rc, strrc(rc)); // 记录警告
      disk_buffer_pool_->unpin_page(frame); // 取消固定父页面
      return rc; // 返回错误
    }

    IndexNodeHandler node(mtr, file_header_, child_frame); // 处理子节点
    if (node.is_leaf()) { // 如果是叶子节点
      rc = print_leaf(child_frame); // 打印叶子节点
    } else {
      rc = print_internal_node_recursive(child_frame); // 递归打印内部节点
    }
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to print node. page id=%d, rc=%d:%s", child_frame->page_num(), rc, strrc(rc)); // 记录警告
      disk_buffer_pool_->unpin_page(frame); // 取消固定父页面
      return rc; // 返回错误
    }
  }

  disk_buffer_pool_->unpin_page(frame); // 取消固定父页面
  return RC::SUCCESS; // 返回成功
}


// ------------------------------------------------

RC BplusTreeHandler::print_tree()
{
  if (disk_buffer_pool_ == nullptr) { // 检查磁盘缓冲池是否为空
    LOG_WARN("Index hasn't been created or opened, fail to print"); // 如果未创建或打开，记录警告
    return RC::SUCCESS; // 返回成功状态
  }
  if (is_empty()) { // 检查树是否为空
    LOG_INFO("tree is empty"); // 记录信息
    return RC::SUCCESS; // 返回成功状态
  }

  Frame  *frame    = nullptr; // 定义frame
  PageNum page_num = file_header_.root_page; // 获取根页面号

  // 获取根页面
  RC rc = disk_buffer_pool_->get_this_page(page_num, &frame);
  if (OB_FAIL(rc)) { // 检查获取页面是否成功
    LOG_WARN("failed to fetch page. page id=%d, rc=%d:%s", page_num, rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  BplusTreeMiniTransaction mtr(*this); // 创建B+树事务

  IndexNodeHandler node(mtr, file_header_, frame); // 创建节点处理器
  if (node.is_leaf()) { // 如果是叶节点
    rc = print_leaf(frame); // 打印叶节点
  } else {
    rc = print_internal_node_recursive(frame); // 打印内部节点
  }
  return rc; // 返回结果
}

RC BplusTreeHandler::print_leafs()
{
  if (is_empty()) { // 检查树是否为空
    LOG_INFO("empty tree"); // 记录信息
    return RC::SUCCESS; // 返回成功状态
  }

  BplusTreeMiniTransaction mtr(*this); // 创建B+树事务
  LatchMemo latch_memo = mtr.latch_memo(); // 获取锁记忆
  Frame    *frame = nullptr; // 定义frame

  // 获取最左边的页面
  RC rc = left_most_page(mtr, frame);
  if (OB_FAIL(rc)) { // 检查获取是否成功
    LOG_WARN("failed to get left most page. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  // 遍历所有叶节点
  while (frame->page_num() != BP_INVALID_PAGE_NUM) {
    LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶节点处理器
    LOG_INFO("leaf info: %s", to_string(leaf_node, key_printer_).c_str()); // 打印叶节点信息

    PageNum next_page_num = leaf_node.next_page(); // 获取下一个页面号
    latch_memo.release(); // 释放锁

    if (next_page_num == BP_INVALID_PAGE_NUM) { // 检查下一个页面号是否有效
      break; // 退出循环
    }

    // 获取下一个页面
    rc = latch_memo.get_page(next_page_num, frame);
    if (OB_FAIL(rc)) { // 检查获取是否成功
      LOG_WARN("failed to get next page. page id=%d, rc=%d:%s", next_page_num, rc, strrc(rc)); // 记录警告
      return rc; // 返回错误代码
    }
  }
  return rc; // 返回结果
}

bool BplusTreeHandler::validate_node_recursive(BplusTreeMiniTransaction &mtr, Frame *frame)
{
  bool             result = true; // 结果初始化为true
  IndexNodeHandler node(mtr, file_header_, frame); // 创建节点处理器
  if (node.is_leaf()) { // 如果是叶节点
    LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶节点处理器
    result = leaf_node.validate(key_comparator_, disk_buffer_pool_); // 验证叶节点
  } else {
    InternalIndexNodeHandler internal_node(mtr, file_header_, frame); // 创建内部节点处理器
    result = internal_node.validate(key_comparator_, disk_buffer_pool_); // 验证内部节点
    for (int i = 0; result && i < internal_node.size(); i++) { // 遍历所有子节点
      PageNum page_num = internal_node.value_at(i); // 获取子节点的页面号
      Frame  *child_frame = nullptr; // 定义子节点frame
      RC      rc = mtr.latch_memo().get_page(page_num, child_frame); // 获取子节点页面
      if (OB_FAIL(rc)) { // 检查获取是否成功
        LOG_WARN("failed to fetch child page.page id=%d, rc=%d:%s", page_num, rc, strrc(rc)); // 记录警告
        result = false; // 设置结果为false
        break; // 退出循环
      }

      result = validate_node_recursive(mtr, child_frame); // 递归验证子节点
    }
  }

  return result; // 返回结果
}

bool BplusTreeHandler::validate_leaf_link(BplusTreeMiniTransaction &mtr)
{
  if (is_empty()) { // 检查树是否为空
    return true; // 如果为空，返回true
  }

  Frame *frame = nullptr; // 定义frame

  // 获取最左边的页面
  RC rc = left_most_page(mtr, frame);
  if (OB_FAIL(rc)) { // 检查获取是否成功
    LOG_WARN("failed to fetch left most page. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return false; // 返回false
  }

  LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶节点处理器
  PageNum              next_page_num = leaf_node.next_page(); // 获取下一个页面号

  // 保存前一个节点的键
  MemPoolItem::item_unique_ptr prev_key = mem_pool_item_->alloc_unique_ptr();
  memcpy(prev_key.get(), leaf_node.key_at(leaf_node.size() - 1), file_header_.key_length);

  bool result = true; // 结果初始化为true
  while (result && next_page_num != BP_INVALID_PAGE_NUM) { // 遍历所有叶节点
    rc = mtr.latch_memo().get_page(next_page_num, frame); // 获取下一个页面
    if (OB_FAIL(rc)) { // 检查获取是否成功
      LOG_WARN("failed to fetch next page. page num=%d, rc=%s", next_page_num, strrc(rc)); // 记录警告
      return false; // 返回false
    }

    LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶节点处理器
    if (key_comparator_((char *)prev_key.get(), leaf_node.key_at(0)) >= 0) { // 检查前一个键是否小于当前第一个键
      LOG_WARN("invalid page. current first key is not bigger than last"); // 记录警告
      result = false; // 设置结果为false
    }

    next_page_num = leaf_node.next_page(); // 更新下一个页面号
    memcpy(prev_key.get(), leaf_node.key_at(leaf_node.size() - 1), file_header_.key_length); // 更新前一个键
  }

  return result; // 返回结果
}

bool BplusTreeHandler::validate_tree()
{
  if (is_empty()) { // 检查树是否为空
    return true; // 如果为空，返回true
  }

  BplusTreeMiniTransaction mtr(*this); // 创建B+树事务
  LatchMemo &latch_memo = mtr.latch_memo(); // 获取锁记忆
  Frame    *frame = nullptr; // 定义frame

  // 获取根页面
  RC rc = latch_memo.get_page(file_header_.root_page, frame); // 这里仅仅调试使用，不加root锁
  if (OB_FAIL(rc)) { // 检查获取是否成功
    LOG_WARN("failed to fetch root page. page id=%d, rc=%d:%s", file_header_.root_page, rc, strrc(rc)); // 记录警告
    return false; // 返回false
  }

  // 验证节点和叶链接
  if (!validate_node_recursive(mtr, frame) || !validate_leaf_link(mtr)) {
    LOG_WARN("Current B+ Tree is invalid"); // 记录警告
    print_tree(); // 打印树结构
    return false; // 返回false
  }

  LOG_INFO("great! current tree is valid"); // 记录信息
  return true; // 返回true
}

bool BplusTreeHandler::is_empty() const { return file_header_.root_page == BP_INVALID_PAGE_NUM; } // 检查树是否为空

RC BplusTreeHandler::find_leaf(BplusTreeMiniTransaction &mtr, BplusTreeOperationType op, const char *key, Frame *&frame)
{
  // 获取子节点页面的函数
  auto child_page_getter = [this, key](InternalIndexNodeHandler &internal_node) {
    return internal_node.value_at(internal_node.lookup(key_comparator_, key)); // 根据键查找子节点
  };
  return find_leaf_internal(mtr, op, child_page_getter, frame); // 调用内部函数查找叶节点
}

RC BplusTreeHandler::left_most_page(BplusTreeMiniTransaction &mtr, Frame *&frame)
{
  // 获取最左边子节点页面的函数
  auto child_page_getter = [](InternalIndexNodeHandler &internal_node) { return internal_node.value_at(0); }; 
  return find_leaf_internal(mtr, BplusTreeOperationType::READ, child_page_getter, frame); // 查找最左边页面
}

RC BplusTreeHandler::find_leaf_internal(BplusTreeMiniTransaction &mtr, BplusTreeOperationType op,
    const function<PageNum(InternalIndexNodeHandler &)> &child_page_getter, Frame *&frame)
{
  LatchMemo &latch_memo = mtr.latch_memo(); // 获取锁记忆

  // 对根节点加锁
  if (op != BplusTreeOperationType::READ) {
    latch_memo.xlatch(&root_lock_); // 独占锁
  } else {
    latch_memo.slatch(&root_lock_); // 共享锁
  }

  if (is_empty()) { // 检查树是否为空
    return RC::EMPTY; // 返回空状态
  }

  // 获取根页面
  RC rc = crabing_protocal_fetch_page(mtr, op, file_header_.root_page, true /* is_root_node */, frame);
  if (OB_FAIL(rc)) { // 检查获取是否成功
    LOG_WARN("failed to fetch root page. page id=%d, rc=%d:%s", file_header_.root_page, rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  IndexNode *node = (IndexNode *)frame->data(); // 获取节点数据
  PageNum    next_page_id; // 定义下一个页面号
  for (; !node->is_leaf;) { // 遍历直到找到叶节点
    InternalIndexNodeHandler internal_node(mtr, file_header_, frame); // 创建内部节点处理器
    next_page_id = child_page_getter(internal_node); // 获取子节点页面号
    rc           = crabing_protocal_fetch_page(mtr, op, next_page_id, false /* is_root_node */, frame); // 获取子节点页面
    if (OB_FAIL(rc)) { // 检查获取是否成功
      LOG_WARN("Failed to load page page_num:%d. rc=%s", next_page_id, strrc(rc)); // 记录警告
      return rc; // 返回错误代码
    }

    node = (IndexNode *)frame->data(); // 更新节点数据
  }
  return RC::SUCCESS; // 返回成功状态
}

RC BplusTreeHandler::crabing_protocal_fetch_page(
    BplusTreeMiniTransaction &mtr, BplusTreeOperationType op, PageNum page_num, bool is_root_node, Frame *&frame)
{
  LatchMemo &latch_memo = mtr.latch_memo(); // 获取锁记忆
  bool      readonly   = (op == BplusTreeOperationType::READ); // 判断是否为只读操作
  const int memo_point = latch_memo.memo_point(); // 记录锁记忆点

  // 获取页面
  RC rc = latch_memo.get_page(page_num, frame);
  if (OB_FAIL(rc)) { // 检查获取是否成功
    LOG_WARN("failed to get frame. pageNum=%d, rc=%s", page_num, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  LatchMemoType latch_type = readonly ? LatchMemoType::SHARED : LatchMemoType::EXCLUSIVE; // 确定锁类型
  mtr.latch_memo().latch(frame, latch_type); // 锁定页面
  IndexNodeHandler index_node(mtr, file_header_, frame); // 创建节点处理器
  if (index_node.is_safe(op, is_root_node)) { // 检查节点是否安全
    latch_memo.release_to(memo_point);  // 如果安全，释放之前的锁
  }
  return rc; // 返回结果
}

RC BplusTreeHandler::insert_entry_into_leaf_node(BplusTreeMiniTransaction &mtr, Frame *frame, const char *key, const RID *rid)
{
  LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶子节点处理器
  bool                 exists          = false;  // 检查数据是否已存在于指定的叶子节点中
  int                  insert_position = leaf_node.lookup(key_comparator_, key, &exists); // 查找插入位置
  if (exists) {
    LOG_TRACE("entry exists"); // 记录日志：条目已存在
    return RC::RECORD_DUPLICATE_KEY; // 返回重复键错误
  }

  if (leaf_node.size() < leaf_node.max_size()) { // 如果叶子节点未满
    leaf_node.insert(insert_position, key, (const char *)rid); // 插入新条目
    frame->mark_dirty(); // 标记页面为脏
    // disk_buffer_pool_->unpin_page(frame); // 取消固定页面由latch memo处理
    return RC::SUCCESS; // 返回成功
  }

  Frame *new_frame = nullptr; // 创建新页面
  RC     rc        = split<LeafIndexNodeHandler>(mtr, frame, new_frame); // 分裂叶子节点
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to split leaf node. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc; // 返回错误
  }

  LeafIndexNodeHandler new_index_node(mtr, file_header_, new_frame); // 创建新叶子节点处理器
  new_index_node.set_next_page(leaf_node.next_page()); // 设置新节点的下一页面
  new_index_node.set_parent_page_num(leaf_node.parent_page_num()); // 设置新节点的父页面编号
  leaf_node.set_next_page(new_frame->page_num()); // 设置当前节点的下一页面为新节点

  if (insert_position < leaf_node.size()) { // 如果插入位置在当前叶子节点中
    leaf_node.insert(insert_position, key, (const char *)rid); // 在当前叶子节点插入
  } else { // 否则在新叶子节点中插入
    new_index_node.insert(insert_position - leaf_node.size(), key, (const char *)rid);
  }

  return insert_entry_into_parent(mtr, frame, new_frame, new_index_node.key_at(0)); // 将新条目插入父节点
}

RC BplusTreeHandler::insert_entry_into_parent(BplusTreeMiniTransaction &mtr, Frame *frame, Frame *new_frame, const char *key)
{
  RC rc = RC::SUCCESS;

  IndexNodeHandler node_handler(mtr, file_header_, frame); // 创建节点处理器
  IndexNodeHandler new_node_handler(mtr, file_header_, new_frame); // 创建新节点处理器
  PageNum          parent_page_num = node_handler.parent_page_num(); // 获取父页面编号

  if (parent_page_num == BP_INVALID_PAGE_NUM) { // 如果没有父页面，即为根节点

    // 创建新的根页面
    Frame *root_frame = nullptr;
    rc = disk_buffer_pool_->allocate_page(&root_frame); // 分配新页面
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to allocate new root page. rc=%d:%s", rc, strrc(rc)); // 记录警告
      return rc; // 返回错误
    }

    InternalIndexNodeHandler root_node(mtr, file_header_, root_frame); // 创建根节点处理器
    root_node.init_empty(); // 初始化为空节点
    root_node.create_new_root(frame->page_num(), key, new_frame->page_num()); // 创建新的根节点
    node_handler.set_parent_page_num(root_frame->page_num()); // 设置原节点的父页面为新根节点
    new_node_handler.set_parent_page_num(root_frame->page_num()); // 设置新节点的父页面为新根节点

    frame->mark_dirty(); // 标记原节点页面为脏
    new_frame->mark_dirty(); // 标记新节点页面为脏
    // disk_buffer_pool_->unpin_page(frame);
    // disk_buffer_pool_->unpin_page(new_frame);

    root_frame->write_latch();  // 更新根页面后，加锁以确保其他线程无法访问
    update_root_page_num_locked(mtr, root_frame->page_num()); // 更新根页面编号
    root_frame->mark_dirty(); // 标记根页面为脏
    root_frame->write_unlatch(); // 解锁根页面
    disk_buffer_pool_->unpin_page(root_frame); // 取消固定根页面

    return RC::SUCCESS; // 返回成功

  } else { // 如果有父页面

    Frame *parent_frame = nullptr; // 创建父页面

    rc = mtr.latch_memo().get_page(parent_page_num, parent_frame); // 获取父页面
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to insert entry into leaf. rc=%d:%s", rc, strrc(rc)); // 记录警告
      // 应该做一些恢复工作
      return rc; // 返回错误
    }

    // 在第一次遍历这个页面时，我们已经拿到parent frame的write latch，所以这里不再加锁
    InternalIndexNodeHandler parent_node(mtr, file_header_, parent_frame); // 创建父节点处理器

    // 当前父节点未满，直接插入新节点
    if (parent_node.size() < parent_node.max_size()) {
      parent_node.insert(key, new_frame->page_num(), key_comparator_); // 在父节点中插入新节点
      new_node_handler.set_parent_page_num(parent_page_num); // 设置新节点的父页面为父节点

      frame->mark_dirty(); // 标记原节点页面为脏
      new_frame->mark_dirty(); // 标记新节点页面为脏
      parent_frame->mark_dirty(); // 标记父节点页面为脏
      // disk_buffer_pool_->unpin_page(frame);
      // disk_buffer_pool_->unpin_page(new_frame);
      // disk_buffer_pool_->unpin_page(parent_frame);

    } else { // 如果父节点即将满，需要分裂操作

      Frame *new_parent_frame = nullptr; // 创建新的父页面

      rc = split<InternalIndexNodeHandler>(mtr, parent_frame, new_parent_frame); // 分裂父节点
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to split internal node. rc=%d:%s", rc, strrc(rc)); // 记录警告
        // disk_buffer_pool_->unpin_page(frame);
        // disk_buffer_pool_->unpin_page(new_frame);
        // disk_buffer_pool_->unpin_page(parent_frame);
      } else {
        // 根据键比较结果决定插入到左侧还是右侧
        InternalIndexNodeHandler new_node(mtr, file_header_, new_parent_frame); // 创建新的内部节点处理器
        if (key_comparator_(key, new_node.key_at(0)) > 0) { // 如果键大于新节点的第一个键
          new_node.insert(key, new_frame->page_num(), key_comparator_); // 在新节点中插入
          new_node_handler.set_parent_page_num(new_node.page_num()); // 设置新节点的父页面为新节点
        } else { // 否则在父节点中插入
          parent_node.insert(key, new_frame->page_num(), key_comparator_);
          new_node_handler.set_parent_page_num(parent_node.page_num()); // 设置新节点的父页面为父节点
        }

        // disk_buffer_pool_->unpin_page(frame);
        // disk_buffer_pool_->unpin_page(new_frame);

        // 虽然这里是递归调用，但B+树通常层数较低，避免栈溢出风险。
        // Q: 在查找叶子节点时，提前释放不必要的锁。在这里插入数据时，是向上遍历节点，
        //    理论上可以释放更低层级节点的锁，但并没有这样做，为什么？
        rc = insert_entry_into_parent(mtr, parent_frame, new_parent_frame, new_node.key_at(0)); // 将新条目插入父节点
      }
    }
  }
  return rc; // 返回结果
}


/**
 * split one full node into two
 */
template <typename IndexNodeHandlerType>
RC BplusTreeHandler::split(BplusTreeMiniTransaction &mtr, Frame *frame, Frame *&new_frame)
{
  IndexNodeHandlerType old_node(mtr, file_header_, frame); // 创建旧节点处理器

  // 分配新节点
  RC rc = mtr.latch_memo().allocate_page(new_frame);
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to split index page due to failed to allocate page, rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  mtr.latch_memo().xlatch(new_frame); // 对新节点加独占锁

  IndexNodeHandlerType new_node(mtr, file_header_, new_frame); // 创建新节点处理器
  new_node.init_empty(); // 初始化新节点
  new_node.set_parent_page_num(old_node.parent_page_num()); // 设置新节点的父节点页号

  old_node.move_half_to(new_node); // 将旧节点的一半数据移动到新节点

  frame->mark_dirty(); // 标记旧节点为脏
  new_frame->mark_dirty(); // 标记新节点为脏
  return RC::SUCCESS; // 返回成功状态
}

RC BplusTreeHandler::recover_update_root_page(BplusTreeMiniTransaction &mtr, PageNum root_page_num)
{
  update_root_page_num_locked(mtr, root_page_num); // 更新根节点页号
  return RC::SUCCESS; // 返回成功状态
}

RC BplusTreeHandler::recover_init_header_page(BplusTreeMiniTransaction &mtr, Frame *frame, const IndexFileHeader &header)
{
  IndexFileHeader *file_header = reinterpret_cast<IndexFileHeader *>(frame->data()); // 获取页数据
  memcpy(file_header, &header, sizeof(IndexFileHeader)); // 复制文件头信息
  file_header_ = header; // 更新文件头
  header_dirty_ = false; // 标记文件头为未脏
  frame->mark_dirty(); // 标记当前页为脏

  key_comparator_.init(file_header_.attr_type, file_header_.attr_length); // 初始化键比较器
  key_printer_.init(file_header_.attr_type, file_header_.attr_length); // 初始化键打印器

  return RC::SUCCESS; // 返回成功状态
}

void BplusTreeHandler::update_root_page_num_locked(BplusTreeMiniTransaction &mtr, PageNum root_page_num)
{
  Frame *frame = nullptr;
  mtr.latch_memo().get_page(FIRST_INDEX_PAGE, frame); // 获取第一索引页
  mtr.latch_memo().xlatch(frame); // 对该页加独占锁
  IndexFileHeader *file_header = reinterpret_cast<IndexFileHeader *>(frame->data()); // 获取页数据
  mtr.logger().update_root_page(frame, root_page_num, file_header->root_page); // 更新日志
  file_header->root_page = root_page_num; // 更新文件头中的根页号
  file_header_.root_page = root_page_num; // 更新成员变量中的根页号
  header_dirty_ = true; // 标记文件头为脏
  frame->mark_dirty(); // 标记当前页为脏
  LOG_DEBUG("set root page to %d", root_page_num); // 记录调试信息
}

RC BplusTreeHandler::create_new_tree(BplusTreeMiniTransaction &mtr, const char *key, const RID *rid)
{
  RC rc = RC::SUCCESS;
  if (file_header_.root_page != BP_INVALID_PAGE_NUM) { // 如果根页号有效
    rc = RC::INTERNAL; // 设置返回状态为内部错误
    LOG_WARN("cannot create new tree while root page is valid. root page id=%d", file_header_.root_page); // 记录警告
    return rc; // 返回错误代码
  }

  Frame *frame = nullptr;

  rc = mtr.latch_memo().allocate_page(frame); // 分配新页
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to allocate root page. rc=%d:%s", rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  LeafIndexNodeHandler leaf_node(mtr, file_header_, frame); // 创建叶节点处理器
  leaf_node.init_empty(); // 初始化叶节点
  leaf_node.insert(0, key, (const char *)rid); // 插入键值对
  update_root_page_num_locked(mtr, frame->page_num()); // 更新根节点页号
  frame->mark_dirty(); // 标记当前页为脏

  return rc; // 返回结果
}

MemPoolItem::item_unique_ptr BplusTreeHandler::make_key(const char *user_key, const RID &rid)
{
  MemPoolItem::item_unique_ptr key = mem_pool_item_->alloc_unique_ptr(); // 分配内存
  if (key == nullptr) {
    LOG_WARN("Failed to alloc memory for key."); // 记录警告
    return nullptr; // 返回空指针
  }
  memcpy(static_cast<char *>(key.get()), user_key, file_header_.attr_length); // 复制用户键
  memcpy(static_cast<char *>(key.get()) + file_header_.attr_length, &rid, sizeof(rid)); // 复制RID
  return key; // 返回生成的键
}

RC BplusTreeHandler::insert_entry(const char *user_key, const RID *rid)
{
  if (user_key == nullptr || rid == nullptr) { // 检查参数有效性
    LOG_WARN("Invalid arguments, key is empty or rid is empty"); // 记录警告
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  MemPoolItem::item_unique_ptr pkey = make_key(user_key, *rid); // 创建键
  if (pkey == nullptr) {
    LOG_WARN("Failed to alloc memory for key."); // 记录警告
    return RC::NOMEM; // 返回内存不足错误
  }

  RC rc = RC::SUCCESS;

  BplusTreeMiniTransaction mtr(*this, &rc); // 创建事务

  char *key = static_cast<char *>(pkey.get()); // 获取键指针

  if (is_empty()) { // 如果树为空
    root_lock_.lock(); // 加锁根节点
    if (is_empty()) { // 再次检查
      rc = create_new_tree(mtr, key, rid); // 创建新树
      root_lock_.unlock(); // 解锁根节点
      return rc; // 返回结果
    }
    root_lock_.unlock(); // 解锁根节点
  }

  Frame *frame = nullptr;

  rc = find_leaf(mtr, BplusTreeOperationType::INSERT, key, frame); // 查找叶节点
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to find leaf %s. rc=%d:%s", rid->to_string().c_str(), rc, strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  rc = insert_entry_into_leaf_node(mtr, frame, key, rid); // 插入到叶节点
  if (OB_FAIL(rc)) {
    LOG_TRACE("Failed to insert into leaf of index, rid:%s. rc=%s", rid->to_string().c_str(), strrc(rc)); // 记录跟踪信息
    return rc; // 返回错误代码
  }

  LOG_TRACE("insert entry success"); // 记录插入成功信息
  return RC::SUCCESS; // 返回成功状态
}

RC BplusTreeHandler::get_entry(const char *user_key, int key_len, list<RID> &rids)
{
  BplusTreeScanner scanner(*this); // 创建扫描器
  RC rc = scanner.open(user_key, key_len, true /*left_inclusive*/, user_key, key_len, true /*right_inclusive*/); // 打开扫描器
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open scanner. rc=%s", strrc(rc)); // 记录警告
    return rc; // 返回错误代码
  }

  RID rid;
  while ((rc = scanner.next_entry(rid)) == RC::SUCCESS) { // 遍历扫描结果
    rids.push_back(rid); // 添加RID到列表
  }

  scanner.close(); // 关闭扫描器
  if (rc != RC::RECORD_EOF) { // 检查是否到达结束
    LOG_WARN("scanner return error. rc=%s", strrc(rc)); // 记录警告
  } else {
    rc = RC::SUCCESS; // 设置返回状态为成功
  }
  return rc; // 返回结果
}

RC BplusTreeHandler::adjust_root(BplusTreeMiniTransaction &mtr, Frame *root_frame)
{
  LatchMemo &latch_memo = mtr.latch_memo(); // 获取锁记忆

  IndexNodeHandler root_node(mtr, file_header_, root_frame); // 创建根节点处理器
  if (root_node.is_leaf() && root_node.size() > 0) { // 如果根节点是叶节点且非空
    root_frame->mark_dirty(); // 标记根节点为脏
    return RC::SUCCESS; // 返回成功状态
  }

  PageNum new_root_page_num = BP_INVALID_PAGE_NUM; // 新根页号初始化
  if (root_node.is_leaf()) { // 如果根节点是叶节点
    ASSERT(root_node.size() == 0, ""); // 断言根节点为空
    new_root_page_num = BP_INVALID_PAGE_NUM; // 设置新根页号为无效
  } else {
    // 根节点只有一个子节点，需要把自己删除，子节点提升为根节点
    InternalIndexNodeHandler internal_node(mtr, file_header_, root_frame); // 创建内部节点处理器

    const PageNum child_page_num = internal_node.value_at(0); // 获取子节点页号
    Frame *child_frame = nullptr;
    RC rc = latch_memo.get_page(child_page_num, child_frame); // 获取子节点页面
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to fetch child page. page num=%d, rc=%d:%s", child_page_num, rc, strrc(rc)); // 记录警告
      return rc; // 返回错误代码
    }

    IndexNodeHandler child_node(mtr, file_header_, child_frame); // 创建子节点处理器
    child_node.set_parent_page_num(BP_INVALID_PAGE_NUM); // 设置子节点父页号为无效

    new_root_page_num = child_page_num; // 设置新根页号
  }

  update_root_page_num_locked(mtr, new_root_page_num); // 更新根节点页号

  PageNum old_root_page_num = root_frame->page_num(); // 获取旧根节点页号
  latch_memo.dispose_page(old_root_page_num); // 释放旧根节点页面
  return RC::SUCCESS; // 返回成功状态
}


template <typename IndexNodeHandlerType>
RC BplusTreeHandler::coalesce_or_redistribute(BplusTreeMiniTransaction &mtr, Frame *frame)
{
  LatchMemo &latch_memo = mtr.latch_memo();

  IndexNodeHandlerType index_node(mtr, file_header_, frame);
  if (index_node.size() >= index_node.min_size()) {
    return RC::SUCCESS;
  }

  const PageNum parent_page_num = index_node.parent_page_num();
  if (BP_INVALID_PAGE_NUM == parent_page_num) {
    // this is the root page
    if (index_node.size() > 1) {
    } else {
      // adjust the root node
      adjust_root(mtr, frame);
    }
    return RC::SUCCESS;
  }

  Frame *parent_frame = nullptr;

  RC rc = latch_memo.get_page(parent_page_num, parent_frame);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to fetch parent page. page id=%d, rc=%d:%s", parent_page_num, rc, strrc(rc));
    return rc;
  }

  InternalIndexNodeHandler parent_index_node(mtr, file_header_, parent_frame);

  int index = parent_index_node.lookup(key_comparator_, index_node.key_at(index_node.size() - 1));
  ASSERT(parent_index_node.value_at(index) == frame->page_num(),
         "lookup return an invalid value. index=%d, this page num=%d, but got %d",
         index, frame->page_num(), parent_index_node.value_at(index));

  PageNum neighbor_page_num;
  if (index == 0) {
    neighbor_page_num = parent_index_node.value_at(1);
  } else {
    neighbor_page_num = parent_index_node.value_at(index - 1);
  }

  Frame *neighbor_frame = nullptr;

  // 当前已经拥有了父节点的写锁，所以直接尝试获取此页面然后加锁
  rc = latch_memo.get_page(neighbor_page_num, neighbor_frame);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to fetch neighbor page. page id=%d, rc=%d:%s", neighbor_page_num, rc, strrc(rc));
    // do something to release resource
    return rc;
  }

  latch_memo.xlatch(neighbor_frame);

  IndexNodeHandlerType neighbor_node(mtr, file_header_, neighbor_frame);
  if (index_node.size() + neighbor_node.size() > index_node.max_size()) {
    rc = redistribute<IndexNodeHandlerType>(mtr, neighbor_frame, frame, parent_frame, index);
  } else {
    rc = coalesce<IndexNodeHandlerType>(mtr, neighbor_frame, frame, parent_frame, index);
  }

  return rc;
}

/**
 * @brief 合并两个节点
 * @details 当某个节点数据量太少时，并且它跟它的邻居加在一起都不超过最大值，就需要跟它旁边的节点做合并。
 * 可能是内部节点，也可能是叶子节点。叶子节点还需要维护next_page指针。
 * @tparam IndexNodeHandlerType 模板类，可能是内部节点，也可能是叶子节点
 * @param mtr mini transaction
 * @param neighbor_frame 要合并的邻居页面
 * @param frame 即将合并的页面
 * @param parent_frame 父节点页面
 * @param index 在父节点的哪个位置
 */
template <typename IndexNodeHandlerType>
RC BplusTreeHandler::coalesce(
    BplusTreeMiniTransaction &mtr, Frame *neighbor_frame, Frame *frame, Frame *parent_frame, int index)
{
  InternalIndexNodeHandler parent_node(mtr, file_header_, parent_frame);

  // 先区分出来左右节点
  Frame *left_frame  = nullptr;
  Frame *right_frame = nullptr;
  if (index == 0) {
    // neighbor node is at right
    left_frame  = frame;
    right_frame = neighbor_frame;
    index++;
  } else {
    left_frame  = neighbor_frame;
    right_frame = frame;
    // neighbor is at left
  }

  // 把右边节点的数据复制到左边节点上去
  IndexNodeHandlerType left_node(mtr, file_header_, left_frame);
  IndexNodeHandlerType right_node(mtr, file_header_, right_frame);

  parent_node.remove(index);
  // parent_node.validate(key_comparator_, disk_buffer_pool_, file_id_);
  RC rc = right_node.move_to(left_node);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to move right node to left. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  // left_node.validate(key_comparator_);

  // 叶子节点维护next_page指针
  if (left_node.is_leaf()) {
    LeafIndexNodeHandler left_leaf_node(mtr, file_header_, left_frame);
    LeafIndexNodeHandler right_leaf_node(mtr, file_header_, right_frame);
    left_leaf_node.set_next_page(right_leaf_node.next_page());
  }

  // 释放右边节点
  mtr.latch_memo().dispose_page(right_frame->page_num());

  // 递归的检查父节点是否需要做合并或者重新分配节点数据
  return coalesce_or_redistribute<InternalIndexNodeHandler>(mtr, parent_frame);
}

template <typename IndexNodeHandlerType>
RC BplusTreeHandler::redistribute(BplusTreeMiniTransaction &mtr, Frame *neighbor_frame, Frame *frame, Frame *parent_frame, int index)
{
  InternalIndexNodeHandler parent_node(mtr, file_header_, parent_frame);
  IndexNodeHandlerType     neighbor_node(mtr, file_header_, neighbor_frame);
  IndexNodeHandlerType     node(mtr, file_header_, frame);
  if (neighbor_node.size() < node.size()) {
    LOG_ERROR("got invalid nodes. neighbor node size %d, this node size %d", neighbor_node.size(), node.size());
  }
  if (index == 0) {
    // the neighbor is at right
    neighbor_node.move_first_to_end(node);
    // neighbor_node.validate(key_comparator_, disk_buffer_pool_, file_id_);
    // node.validate(key_comparator_, disk_buffer_pool_, file_id_);
    parent_node.set_key_at(index + 1, neighbor_node.key_at(0));
    // parent_node.validate(key_comparator_, disk_buffer_pool_, file_id_);
  } else {
    // the neighbor is at left
    neighbor_node.move_last_to_front(node);
    // neighbor_node.validate(key_comparator_, disk_buffer_pool_, file_id_);
    // node.validate(key_comparator_, disk_buffer_pool_, file_id_);
    parent_node.set_key_at(index, node.key_at(0));
    // parent_node.validate(key_comparator_, disk_buffer_pool_, file_id_);
  }

  neighbor_frame->mark_dirty();
  frame->mark_dirty();
  parent_frame->mark_dirty();

  return RC::SUCCESS;
}

RC BplusTreeHandler::delete_entry_internal(BplusTreeMiniTransaction &mtr, Frame *leaf_frame, const char *key)
{
  LeafIndexNodeHandler leaf_index_node(mtr, file_header_, leaf_frame);

  const int remove_count = leaf_index_node.remove(key, key_comparator_);
  if (remove_count == 0) {
    LOG_TRACE("no data need to remove");
    // disk_buffer_pool_->unpin_page(leaf_frame);
    return RC::RECORD_NOT_EXIST;
  }
  // leaf_index_node.validate(key_comparator_, disk_buffer_pool_, file_id_);

  leaf_frame->mark_dirty();

  if (leaf_index_node.size() >= leaf_index_node.min_size()) {
    return RC::SUCCESS;
  }

  return coalesce_or_redistribute<LeafIndexNodeHandler>(mtr, leaf_frame);
}

RC BplusTreeHandler::delete_entry(const char *user_key, const RID *rid)
{
  MemPoolItem::item_unique_ptr pkey = mem_pool_item_->alloc_unique_ptr();
  if (nullptr == pkey) {
    LOG_WARN("Failed to alloc memory for key. size=%d", file_header_.key_length);
    return RC::NOMEM;
  }
  char *key = static_cast<char *>(pkey.get());

  memcpy(key, user_key, file_header_.attr_length);
  memcpy(key + file_header_.attr_length, rid, sizeof(*rid));

  BplusTreeOperationType op = BplusTreeOperationType::DELETE;

  RC rc = RC::SUCCESS;

  BplusTreeMiniTransaction mtr(*this, &rc);

  Frame *leaf_frame = nullptr;

  rc = find_leaf(mtr, op, key, leaf_frame);
  if (rc == RC::EMPTY) {
    rc = RC::RECORD_NOT_EXIST;
    return rc;
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to find leaf page. rc =%s", strrc(rc));
    return rc;
  }

  rc = delete_entry_internal(mtr, leaf_frame, key);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

BplusTreeScanner::BplusTreeScanner(BplusTreeHandler &tree_handler)
    : tree_handler_(tree_handler), mtr_(tree_handler)
{}

BplusTreeScanner::~BplusTreeScanner() { close(); }

RC BplusTreeScanner::open(const char *left_user_key, int left_len, bool left_inclusive, const char *right_user_key,
    int right_len, bool right_inclusive)
{
  RC rc = RC::SUCCESS;
  if (inited_) {
    LOG_WARN("tree scanner has been inited");
    return RC::INTERNAL;
  }

  inited_        = true;
  first_emitted_ = false;

  LatchMemo &latch_memo = mtr_.latch_memo();

  // 校验输入的键值是否是合法范围
  if (left_user_key && right_user_key) {
    const auto &attr_comparator = tree_handler_.key_comparator_.attr_comparator();
    const int   result          = attr_comparator(left_user_key, right_user_key);
    if (result > 0 ||  // left < right
                       // left == right but is (left,right)/[left,right) or (left,right]
        (result == 0 && (left_inclusive == false || right_inclusive == false))) {
      return RC::INVALID_ARGUMENT;
    }
  }

  if (nullptr == left_user_key) {
    rc = tree_handler_.left_most_page(mtr_, current_frame_);
    if (OB_FAIL(rc)) {
      if (rc == RC::EMPTY) {
        current_frame_ = nullptr;
        return RC::SUCCESS;
      }
      
      LOG_WARN("failed to find left most page. rc=%s", strrc(rc));
      return rc;
    }

    iter_index_ = 0;
  } else {

    char *fixed_left_key = const_cast<char *>(left_user_key);
    if (tree_handler_.file_header_.attr_type == AttrType::CHARS) {
      bool should_inclusive_after_fix = false;
      rc = fix_user_key(left_user_key, left_len, true /*greater*/, &fixed_left_key, &should_inclusive_after_fix);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to fix left user key. rc=%s", strrc(rc));
        return rc;
      }

      if (should_inclusive_after_fix) {
        left_inclusive = true;
      }
    }

    MemPoolItem::item_unique_ptr left_pkey;
    if (left_inclusive) {
      left_pkey = tree_handler_.make_key(fixed_left_key, *RID::min());
    } else {
      left_pkey = tree_handler_.make_key(fixed_left_key, *RID::max());
    }

    const char *left_key = (const char *)left_pkey.get();

    if (fixed_left_key != left_user_key) {
      delete[] fixed_left_key;
      fixed_left_key = nullptr;
    }

    rc = tree_handler_.find_leaf(mtr_, BplusTreeOperationType::READ, left_key, current_frame_);
    if (rc == RC::EMPTY) {
      rc             = RC::SUCCESS;
      current_frame_ = nullptr;
      return rc;
    } else if (OB_FAIL(rc)) {
      LOG_WARN("failed to find left page. rc=%s", strrc(rc));
      return rc;
    }

    LeafIndexNodeHandler left_node(mtr_, tree_handler_.file_header_, current_frame_);
    int                  left_index = left_node.lookup(tree_handler_.key_comparator_, left_key);
    // lookup 返回的是适合插入的位置，还需要判断一下是否在合适的边界范围内
    if (left_index >= left_node.size()) {  // 超出了当前页，就需要向后移动一个位置
      const PageNum next_page_num = left_node.next_page();
      if (next_page_num == BP_INVALID_PAGE_NUM) {  // 这里已经是最后一页，说明当前扫描，没有数据
        latch_memo.release();
        current_frame_ = nullptr;
        return RC::SUCCESS;
      }

      rc = latch_memo.get_page(next_page_num, current_frame_);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to fetch next page. page num=%d, rc=%s", next_page_num, strrc(rc));
        return rc;
      }
      latch_memo.slatch(current_frame_);

      left_index = 0;
    }
    iter_index_ = left_index;
  }

  // 没有指定右边界范围，那么就返回右边界最大值
  if (nullptr == right_user_key) {
    right_key_ = nullptr;
  } else {

    char *fixed_right_key          = const_cast<char *>(right_user_key);
    bool  should_include_after_fix = false;
    if (tree_handler_.file_header_.attr_type == AttrType::CHARS) {
      rc = fix_user_key(right_user_key, right_len, false /*want_greater*/, &fixed_right_key, &should_include_after_fix);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to fix right user key. rc=%s", strrc(rc));
        return rc;
      }

      if (should_include_after_fix) {
        right_inclusive = true;
      }
    }
    if (right_inclusive) {
      right_key_ = tree_handler_.make_key(fixed_right_key, *RID::max());
    } else {
      right_key_ = tree_handler_.make_key(fixed_right_key, *RID::min());
    }

    if (fixed_right_key != right_user_key) {
      delete[] fixed_right_key;
      fixed_right_key = nullptr;
    }
  }

  if (touch_end()) {
    current_frame_ = nullptr;
  }

  return RC::SUCCESS;
}

void BplusTreeScanner::fetch_item(RID &rid)
{
  LeafIndexNodeHandler node(mtr_, tree_handler_.file_header_, current_frame_);
  memcpy(&rid, node.value_at(iter_index_), sizeof(rid));
}

bool BplusTreeScanner::touch_end()
{
  if (right_key_ == nullptr) {
    return false;
  }

  LeafIndexNodeHandler node(mtr_, tree_handler_.file_header_, current_frame_);

  const char *this_key       = node.key_at(iter_index_);
  int         compare_result = tree_handler_.key_comparator_(this_key, static_cast<char *>(right_key_.get()));
  return compare_result > 0;
}

RC BplusTreeScanner::next_entry(RID &rid)
{
  if (nullptr == current_frame_) {
    return RC::RECORD_EOF;
  }

  if (!first_emitted_) {
    fetch_item(rid);
    first_emitted_ = true;
    return RC::SUCCESS;
  }

  iter_index_++;

  LeafIndexNodeHandler node(mtr_, tree_handler_.file_header_, current_frame_);
  if (iter_index_ < node.size()) {
    if (touch_end()) {
      return RC::RECORD_EOF;
    }

    fetch_item(rid);
    return RC::SUCCESS;
  }

  RC      rc            = RC::SUCCESS;
  PageNum next_page_num = node.next_page();
  if (BP_INVALID_PAGE_NUM == next_page_num) {
    return RC::RECORD_EOF;
  }

  LatchMemo &latch_memo = mtr_.latch_memo();

  const int memo_point = latch_memo.memo_point();
  rc                   = latch_memo.get_page(next_page_num, current_frame_);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next page. page num=%d, rc=%s", next_page_num, strrc(rc));
    return rc;
  }

  /**
   * 如果这里直接去加锁，那可能会造成死锁
   * 因为这里访问页面的方式顺序与插入、删除的顺序不一样
   * 如果加锁失败，就由上层做重试
   */
  bool locked = latch_memo.try_slatch(current_frame_);
  if (!locked) {
    return RC::LOCKED_NEED_WAIT;
  }

  latch_memo.release_to(memo_point);
  iter_index_ = -1;  // `next` will add 1
  return next_entry(rid);
}

RC BplusTreeScanner::close()
{
  inited_ = false;
  LOG_TRACE("bplus tree scanner closed");
  return RC::SUCCESS;
}

RC BplusTreeScanner::fix_user_key(
    const char *user_key, int key_len, bool want_greater, char **fixed_key, bool *should_inclusive)
{
  if (nullptr == fixed_key || nullptr == should_inclusive) {
    return RC::INVALID_ARGUMENT;
  }

  // 这里很粗暴，变长字段才需要做调整，其它默认都不需要做调整
  assert(tree_handler_.file_header_.attr_type == AttrType::CHARS);
  assert(strlen(user_key) >= static_cast<size_t>(key_len));

  *should_inclusive = false;

  int32_t attr_length = tree_handler_.file_header_.attr_length;
  char   *key_buf     = new char[attr_length];
  if (nullptr == key_buf) {
    return RC::NOMEM;
  }

  if (key_len <= attr_length) {
    memcpy(key_buf, user_key, key_len);
    memset(key_buf + key_len, 0, attr_length - key_len);

    *fixed_key = key_buf;
    return RC::SUCCESS;
  }

  // key_len > attr_length
  memcpy(key_buf, user_key, attr_length);

  char c = user_key[attr_length];
  if (c == 0) {
    *fixed_key = key_buf;
    return RC::SUCCESS;
  }

  // 扫描 >=/> user_key 的数据
  // 示例：>=/> ABCD1 的数据，attr_length=4,
  //      等价于扫描 >= ABCE 的数据
  // 如果是扫描 <=/< user_key的数据
  // 示例：<=/< ABCD1  <==> <= ABCD  (attr_length=4)
  // NOTE: 假设都是普通的ASCII字符，不包含二进制字符，使用char不会溢出
  *should_inclusive = true;
  if (want_greater) {
    key_buf[attr_length - 1]++;
  }

  *fixed_key = key_buf;
  return RC::SUCCESS;
}
