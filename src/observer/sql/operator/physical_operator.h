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
// Created by WangYunlai on 2022/6/7.
//

// 防止头文件重复包含的宏定义
#pragma once

#include <memory>  // 引入智能指针支持
#include <string>  // 引入字符串支持
#include <vector>  // 引入向量容器

#include "common/rc.h"       // 引入返回码定义
#include "sql/expr/tuple.h"  // 引入元组相关定义

class Record;         // 前向声明Record类
class TupleCellSpec;  // 前向声明TupleCellSpec类
class Trx;            // 前向声明Trx类

/**
 * @brief 物理算子
 * @defgroup PhysicalOperator
 * @details 物理算子描述执行计划将如何执行，比如从表中怎么获取数据，如何做投影，怎么做连接等
 */

/**
 * @brief 物理算子类型
 * @ingroup PhysicalOperator
 */
enum class PhysicalOperatorType
{
  TABLE_SCAN,        ///< 表扫描
  TABLE_SCAN_VEC,    ///< 矢量化表扫描
  INDEX_SCAN,        ///< 索引扫描
  NESTED_LOOP_JOIN,  ///< 嵌套循环连接
  EXPLAIN,           ///< 执行计划解释
  PREDICATE,         ///< 谓词
  PREDICATE_VEC,     ///< 矢量化谓词
  PROJECT,           ///< 投影操作
  PROJECT_VEC,       ///< 矢量化投影
  CALC,              ///< 计算操作
  STRING_LIST,       ///< 字符串列表
  DELETE,            ///< 删除操作
  INSERT,            ///< 插入操作
  SCALAR_GROUP_BY,   ///< 标量分组
  HASH_GROUP_BY,     ///< 哈希分组
  GROUP_BY_VEC,      ///< 矢量化分组
  AGGREGATE_VEC,     ///< 矢量化聚合
  EXPR_VEC,          ///< 矢量化表达式
};

/**
 * @brief 与LogicalOperator对应，物理算子描述执行计划将如何执行
 * @ingroup PhysicalOperator
 */
class PhysicalOperator
{
public:
  // 默认构造函数
  PhysicalOperator() = default;

  // 默认析构函数
  virtual ~PhysicalOperator() = default;

  /**
   * @brief 获取物理算子的名称
   * @return 返回物理算子的名称
   */
  virtual std::string name() const;

  /**
   * @brief 获取物理算子的参数
   * @return 返回物理算子的参数描述
   */
  virtual std::string param() const;

  /**
   * @brief 获取物理算子的类型
   * @return 返回物理算子的类型
   */
  virtual PhysicalOperatorType type() const = 0;

  /**
   * @brief 打开物理算子，准备执行
   * @param trx 当前的事务
   * @return 返回执行状态
   */
  virtual RC open(Trx *trx) = 0;

  /**
   * @brief 获取下一个结果
   * @return 返回执行状态，默认为未实现
   */
  virtual RC next() { return RC::UNIMPLEMENTED; }

  /**
   * @brief 获取下一个结果并将其存入指定的Chunk中
   * @param chunk 用于存储结果的Chunk
   * @return 返回执行状态，默认为未实现
   */
  virtual RC next(Chunk &chunk) { return RC::UNIMPLEMENTED; }

  /**
   * @brief 关闭物理算子，释放资源
   * @return 返回执行状态
   */
  virtual RC close() = 0;

  /**
   * @brief 获取当前元组
   * @return 返回当前元组的指针，默认为nullptr
   */
  virtual Tuple *current_tuple() { return nullptr; }

  /**
   * @brief 获取元组模式
   * @param schema 用于存储元组模式的引用
   * @return 返回执行状态，默认为未实现
   */
  virtual RC tuple_schema(TupleSchema &schema) const { return RC::UNIMPLEMENTED; }

  /**
   * @brief 添加子物理算子
   * @param oper 要添加的子物理算子
   */
  void add_child(std::unique_ptr<PhysicalOperator> oper)
  {
    // 将新的子物理算子移动到children_向量中
    children_.emplace_back(std::move(oper));
  }

  /**
   * @brief 获取所有子物理算子
   * @return 返回子物理算子向量的引用
   */
  std::vector<std::unique_ptr<PhysicalOperator>> &children() { return children_; }

protected:
  std::vector<std::unique_ptr<PhysicalOperator>> children_;  ///< 子物理算子的唯一指针向量
};
