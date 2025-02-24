/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_hash_table.h"

// ----------------------------------StandardAggregateHashTable------------------

/**
 * @brief 将数据块添加到聚合哈希表中
 *
 * @param groups_chunk  包含分组值的块
 * @param aggrs_chunk   包含聚合值的块
 * @return RC          返回操作结果
 */
RC StandardAggregateHashTable::add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk)
{
  // 这里实现您的代码
  exit(-1);  // 暂未实现
}

/**
 * @brief 打开聚合哈希表的扫描器
 */
void StandardAggregateHashTable::Scanner::open_scan()
{
  it_  = static_cast<StandardAggregateHashTable *>(hash_table_)->begin();  // 初始化迭代器
  end_ = static_cast<StandardAggregateHashTable *>(hash_table_)->end();    // 获取结束迭代器
}

/**
 * @brief 执行下一次扫描并填充输出块
 *
 * @param output_chunk 输出块，用于存放扫描结果
 * @return RC        返回操作结果
 */
RC StandardAggregateHashTable::Scanner::next(Chunk &output_chunk)
{
  if (it_ == end_) {
    return RC::RECORD_EOF;  // 到达结束
  }
  // 在迭代器未到达末尾且输出块容量未满时填充输出块
  while (it_ != end_ && output_chunk.rows() <= output_chunk.capacity()) {
    auto &group_by_values = it_->first;   // 获取分组值
    auto &aggrs           = it_->second;  // 获取聚合值
    for (int i = 0; i < output_chunk.column_num(); i++) {
      auto col_idx = output_chunk.column_ids(i);  // 获取列索引
      if (col_idx >= static_cast<int>(group_by_values.size())) {
        // 填充聚合值
        output_chunk.column(i).append_one((char *)aggrs[col_idx - group_by_values.size()].data());
      } else {
        // 填充分组值
        output_chunk.column(i).append_one((char *)group_by_values[col_idx].data());
      }
    }
    it_++;  // 移动到下一个元素
  }
  if (it_ == end_) {
    return RC::SUCCESS;  // 成功
  }

  return RC::SUCCESS;  // 成功
}

/**
 * @brief 计算给定值向量的哈希值
 *
 * @param vec 输入值向量
 * @return size_t 哈希值
 */
size_t StandardAggregateHashTable::VectorHash::operator()(const vector<Value> &vec) const
{
  size_t hash = 0;
  for (const auto &elem : vec) {
    hash ^= std::hash<string>()(elem.to_string());  // 累加哈希值
  }
  return hash;  // 返回计算出的哈希值
}

/**
 * @brief 比较两个值向量是否相等
 *
 * @param lhs 左侧值向量
 * @param rhs 右侧值向量
 * @return bool 相等返回 true，不相等返回 false
 */
bool StandardAggregateHashTable::VectorEqual::operator()(const vector<Value> &lhs, const vector<Value> &rhs) const
{
  if (lhs.size() != rhs.size()) {
    return false;  // 大小不相等则不相等
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (rhs[i].compare(lhs[i]) != 0) {
      return false;  // 存在不相等的元素
    }
  }
  return true;  // 相等
}

// ----------------------------------LinearProbingAggregateHashTable------------------
#ifdef USE_SIMD

/**
 * @brief 将数据块添加到线性探测聚合哈希表中
 *
 * @param group_chunk 包含分组值的块
 * @param aggr_chunk  包含聚合值的块
 * @return RC        返回操作结果
 */
template <typename V>
RC LinearProbingAggregateHashTable<V>::add_chunk(Chunk &group_chunk, Chunk &aggr_chunk)
{
  if (group_chunk.column_num() != 1 || aggr_chunk.column_num() != 1) {
    LOG_WARN("group_chunk and aggr_chunk size must be 1.");
    return RC::INVALID_ARGUMENT;  // 列数不合法
  }
  if (group_chunk.rows() != aggr_chunk.rows()) {
    LOG_WARN("group_chunk and aggr_chunk rows must be equal.");
    return RC::INVALID_ARGUMENT;  // 行数不一致
  }
  add_batch((int *)group_chunk.column(0).data(), (V *)aggr_chunk.column(0).data(), group_chunk.rows());
  return RC::SUCCESS;  // 成功
}

/**
 * @brief 打开线性探测聚合哈希表的扫描器
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::open_scan()
{
  capacity_   = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->capacity();  // 获取容量
  size_       = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->size();      // 获取大小
  scan_pos_   = 0;                                                                        // 初始化扫描位置
  scan_count_ = 0;                                                                        // 初始化扫描计数
}

/**
 * @brief 执行下一次扫描并填充输出块
 *
 * @param output_chunk 输出块，用于存放扫描结果
 * @return RC        返回操作结果
 */
template <typename V>
RC LinearProbingAggregateHashTable<V>::Scanner::next(Chunk &output_chunk)
{
  if (scan_pos_ >= capacity_ || scan_count_ >= size_) {
    return RC::RECORD_EOF;  // 到达结束
  }
  auto linear_probing_hash_table = static_cast<LinearProbingAggregateHashTable *>(hash_table_);
  while (scan_pos_ < capacity_ && scan_count_ < size_ && output_chunk.rows() <= output_chunk.capacity()) {
    int key;
    V   value;
    RC  rc = linear_probing_hash_table->iter_get(scan_pos_, key, value);
    if (rc == RC::SUCCESS) {
      output_chunk.column(0).append_one((char *)&key);    // 填充键
      output_chunk.column(1).append_one((char *)&value);  // 填充值
      scan_count_++;
    }
    scan_pos_++;
  }
  return RC::SUCCESS;  // 成功
}

/**
 * @brief 关闭扫描器
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::close_scan()
{
  capacity_   = -1;  // 关闭后重置容量
  size_       = -1;  // 关闭后重置大小
  scan_pos_   = -1;  // 关闭后重置扫描位置
  scan_count_ = 0;   // 关闭后重置扫描计数
}

/**
 * @brief 从哈希表中获取指定键的值
 *
 * @param key 要查找的键
 * @param value 输出值
 * @return RC 返回操作结果
 */
template <typename V>
RC LinearProbingAggregateHashTable<V>::get(int key, V &value)
{
  RC  rc          = RC::SUCCESS;
  int index       = (key % capacity_ + capacity_) % capacity_;  // 计算索引
  int iterate_cnt = 0;                                          // 迭代计数
  while (true) {
    if (keys_[index] == EMPTY_KEY) {
      rc = RC::NOT_EXIST;  // 不存在
      break;
    } else if (keys_[index] == key) {
      value = values_[index];  // 找到键对应的值
      break;
    } else {
      index += 1;          // 线性探测
      index %= capacity_;  // 处理索引回绕
      iterate_cnt++;
      if (iterate_cnt > capacity_) {
        rc = RC::NOT_EXIST;  // 超出容量，不存在
        break;
      }
    }
  }
  return rc;  // 返回操作结果
}

/**
 * @brief 从指定位置获取键值对
 *
 * @param pos 位置索引
 * @param key 输出键
 * @param value 输出值
 * @return RC 返回操作结果
 */
template <typename V>
RC LinearProbingAggregateHashTable<V>::iter_get(int pos, int &key, V &value)
{
  RC rc = RC::SUCCESS;
  if (keys_[pos] == LinearProbingAggregateHashTable<V>::EMPTY_KEY) {
    rc = RC::NOT_EXIST;  // 不存在
  } else {
    key   = keys_[pos];    // 赋值键
    value = values_[pos];  // 赋值值
  }
  return rc;  // 返回操作结果
}

/**
 * @brief 聚合值到指定值
 *
 * @param value 当前值
 * @param value_to_aggregate 要聚合的值
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::aggregate(V *value, V value_to_aggregate)
{
  if (aggregate_type_ == AggregateExpr::Type::SUM) {
    *value += value_to_aggregate;  // 执行加法聚合
  } else {
    ASSERT(false, "unsupported aggregate type");  // 不支持的聚合类型
  }
}

/**
 * @brief 扩大哈希表容量
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::resize()
{
  capacity_ *= 2;                          // 容量翻倍
  std::vector<int> new_keys(capacity_);    // 新键数组
  std::vector<V>   new_values(capacity_);  // 新值数组

  // 迁移当前键值对到新的哈希表
  for (size_t i = 0; i < keys_.size(); i++) {
    auto &key   = keys_[i];
    auto &value = values_[i];
    if (key != EMPTY_KEY) {
      int index = (key % capacity_ + capacity_) % capacity_;  // 计算新的索引
      while (new_keys[index] != EMPTY_KEY) {
        index = (index + 1) % capacity_;  // 线性探测
      }
      new_keys[index]   = key;    // 存储新键
      new_values[index] = value;  // 存储新值
    }
  }

  keys_   = std::move(new_keys);    // 更新键数组
  values_ = std::move(new_values);  // 更新值数组
}

/**
 * @brief 检查是否需要扩容
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::resize_if_need()
{
  if (size_ >= capacity_ / 2) {
    resize();  // 需要扩容，调用扩容函数
  }
}

/**
 * @brief 批量添加键值对到哈希表
 *
 * @param input_keys 输入键数组
 * @param input_values 输入值数组
 * @param len 输入数组长度
 */
template <typename V>
void LinearProbingAggregateHashTable<V>::add_batch(int *input_keys, V *input_values, int len)
{
  // 这里实现您的代码
  exit(-1);  // 暂未实现

  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0

  // for (; i + SIMD_WIDTH <= len;) {
  // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
  // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数
  // 3. 计算 hash 值，
  // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i]
  // 在哈希表中的位置，则不更新聚合结果。
  // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
  // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
  // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] =
  // 0，表示该位置在下次循环中不需要读取新的键值对。 如果本次循环中，key[i]聚合完成，则off[i] 更新为
  // 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
  // }
  // 7. 通过标量线性探测，处理剩余键值对

  // resize_if_need(); // 可能需要扩容
}

/**
 * @brief 常量定义
 */
template <typename V>
const int LinearProbingAggregateHashTable<V>::EMPTY_KEY = 0xffffffff;  // 定义空键
template <typename V>
const int LinearProbingAggregateHashTable<V>::DEFAULT_CAPACITY = 16384;  // 定义默认容量

// 实例化模板类
template class LinearProbingAggregateHashTable<int>;
template class LinearProbingAggregateHashTable<float>;

#endif  // USE_SIMD
