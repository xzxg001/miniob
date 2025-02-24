/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <vector>
#include <iostream>
#include <unordered_map>

#include "common/math/simd_util.h"
#include "common/rc.h"
#include "sql/expr/expression.h"

/**
 * @brief 用于 hash group by 的哈希表实现，不支持并发访问。
 * 该基类定义了哈希表的基本接口，包括添加数据和扫描聚合结果的功能。
 */
class AggregateHashTable
{
public:
  class Scanner
  {
  public:
    /**
     * @brief 构造函数
     * @param hash_table 指向要扫描的哈希表的指针
     */
    explicit Scanner(AggregateHashTable *hash_table) : hash_table_(hash_table) {}
    virtual ~Scanner() = default;  // 默认析构函数

    /**
     * @brief 打开扫描操作，准备从哈希表中提取聚合结果。
     */
    virtual void open_scan() = 0;

    /**
     * @brief 通过扫描哈希表，将哈希表中的聚合结果写入 chunk 中。
     * @param chunk 要写入的输出块
     * @return RC 返回操作结果
     */
    virtual RC next(Chunk &chunk) = 0;

    virtual void close_scan() {};  // 关闭扫描操作

  protected:
    AggregateHashTable *hash_table_;  // 指向父哈希表的指针
  };

  /**
   * @brief 将 groups_chunk 和 aggrs_chunk 写入到哈希表中。哈希表中记录了聚合结果。
   * @param groups_chunk 包含分组数据的块
   * @param aggrs_chunk 包含聚合数据的块
   * @return RC 返回操作结果
   */
  virtual RC add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk) = 0;

  virtual ~AggregateHashTable() = default;  // 虚析构函数
};

/**
 * @brief 标准哈希表实现，使用 STL 的 unordered_map。
 * 支持根据分组值进行聚合计算。
 */
class StandardAggregateHashTable : public AggregateHashTable
{
private:
  // 哈希函数，定义如何计算 vector<Value> 的哈希值
  struct VectorHash
  {
    std::size_t operator()(const std::vector<Value> &vec) const;
  };

  // 相等性检查，定义如何判断两个 vector<Value> 是否相等
  struct VectorEqual
  {
    bool operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const;
  };

public:
  using StandardHashTable =
      std::unordered_map<std::vector<Value>, std::vector<Value>, VectorHash, VectorEqual>;  // 自定义哈希表类型

  class Scanner : public AggregateHashTable::Scanner
  {
  public:
    explicit Scanner(AggregateHashTable *hash_table) : AggregateHashTable::Scanner(hash_table) {}
    ~Scanner() = default;  // 默认析构函数

    void open_scan() override;  // 打开扫描

    /**
     * @brief 获取哈希表中的下一个聚合结果并写入指定的 chunk。
     * @param chunk 要写入的输出块
     * @return RC 返回操作结果
     */
    RC next(Chunk &chunk) override;

  private:
    StandardHashTable::iterator end_;  // 哈希表结束迭代器
    StandardHashTable::iterator it_;   // 哈希表当前迭代器
  };

  /**
   * @brief 构造函数
   * @param aggregations 包含聚合表达式的向量
   */
  StandardAggregateHashTable(const std::vector<Expression *> aggregations)
  {
    for (auto &expr : aggregations) {
      ASSERT(expr->type() == ExprType::AGGREGATION, "expect aggregate expression");  // 确保表达式类型为聚合
      auto *aggregation_expr = static_cast<AggregateExpr *>(expr);
      aggr_types_.push_back(aggregation_expr->aggregate_type());  // 保存聚合类型
    }
  }

  virtual ~StandardAggregateHashTable() {}  // 默认析构函数

  /**
   * @brief 将指定的块添加到哈希表中。
   * @param groups_chunk 包含分组数据的块
   * @param aggrs_chunk 包含聚合数据的块
   * @return RC 返回操作结果
   */
  RC add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk) override;

  /**
   * @brief 返回哈希表的开始迭代器
   * @return StandardHashTable::iterator 哈希表开始迭代器
   */
  StandardHashTable::iterator begin() { return aggr_values_.begin(); }

  /**
   * @brief 返回哈希表的结束迭代器
   * @return StandardHashTable::iterator 哈希表结束迭代器
   */
  StandardHashTable::iterator end() { return aggr_values_.end(); }

private:
  /// group by 值到聚合值的映射
  StandardHashTable                aggr_values_;  // 存储聚合值的哈希表
  std::vector<AggregateExpr::Type> aggr_types_;   // 存储聚合表达式类型
};

/**
 * @brief 线性探测哈希表实现。
 * @note 当前只支持 group by 列为 char/char(4) 类型，且聚合列为单列。
 */
#ifdef USE_SIMD
template <typename V>
class LinearProbingAggregateHashTable : public AggregateHashTable
{
public:
  class Scanner : public AggregateHashTable::Scanner
  {
  public:
    explicit Scanner(AggregateHashTable *hash_table) : AggregateHashTable::Scanner(hash_table) {}
    ~Scanner() = default;  // 默认析构函数

    void open_scan() override;  // 打开扫描

    /**
     * @brief 获取哈希表中的下一个聚合结果并写入指定的 chunk。
     * @param chunk 要写入的输出块
     * @return RC 返回操作结果
     */
    RC next(Chunk &chunk) override;

    void close_scan() override;  // 关闭扫描操作

  private:
    int capacity_   = -1;  // 哈希表容量
    int size_       = -1;  // 哈希表当前大小
    int scan_pos_   = -1;  // 当前扫描位置
    int scan_count_ = 0;   // 已扫描的数量
  };

  /**
   * @brief 构造函数
   * @param aggregate_type 指定的聚合类型
   * @param capacity 哈希表的初始容量，默认为 DEFAULT_CAPACITY
   */
  LinearProbingAggregateHashTable(AggregateExpr::Type aggregate_type, int capacity = DEFAULT_CAPACITY)
      : keys_(capacity, EMPTY_KEY), values_(capacity, 0), capacity_(capacity), aggregate_type_(aggregate_type)
  {}

  virtual ~LinearProbingAggregateHashTable() {}  // 默认析构函数

  /**
   * @brief 获取指定键的值
   * @param key 要查找的键
   * @param value 输出的值
   * @return RC 返回操作结果
   */
  RC get(int key, V &value);

  /**
   * @brief 从指定位置获取键值对
   * @param pos 要查找的位置
   * @param key 输出的键
   * @param value 输出的值
   * @return RC 返回操作结果
   */
  RC iter_get(int pos, int &key, V &value);

  /**
   * @brief 将指定的块添加到哈希表中。
   * @param group_chunk 包含分组数据的块
   * @param aggr_chunk 包含聚合数据的块
   * @return RC 返回操作结果
   */
  RC add_chunk(Chunk &group_chunk, Chunk &aggr_chunk) override;

  /**
   * @brief 返回哈希表的容量
   * @return int 哈希表的容量
   */
  int capacity() { return capacity_; }

  /**
   * @brief 返回哈希表的大小
   * @return int 哈希表的大小
   */
  int size() { return size_; }

private:
  /**
   * @brief 将键值对以批量的形式写入哈希表中，参考了论文
   * `Rethinking SIMD Vectorization for In-Memory Databases` 中的 `Algorithm 5`。
   * @param input_keys 输入的键数组
   * @param input_values 输入的值数组，与键数组一一对应。
   * @param len 键值对数组的长度
   */
  void add_batch(int *input_keys, V *input_values, int len);

  /**
   * @brief 聚合操作，将值合并到指定的值中。
   * @param value 当前的值
   * @param value_to_aggregate 要聚合的值
   */
  void aggregate(V *value, V value_to_aggregate);

  /**
   * @brief 重新调整哈希表的大小，以适应更多的元素。
   */
  void resize();

  /**
   * @brief 如果需要，则调整哈希表的大小。
   */
  void resize_if_need();

private:
  static const int EMPTY_KEY;         // 表示空槽位的键值
  static const int DEFAULT_CAPACITY;  // 默认的哈希表初始容量

  std::vector<int>    keys_;            // 存储哈希表的键
  std::vector<V>      values_;          // 存储哈希表的值
  int                 size_     = 0;    // 哈希表当前大小
  int                 capacity_ = 0;    // 哈希表容量
  AggregateExpr::Type aggregate_type_;  // 聚合类型
};
#endif
