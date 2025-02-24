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
// Created by Wangyunlai on 2024/5/31.
//

#include "sql/expr/composite_tuple.h"
#include "common/log/log.h"

using namespace std;

/**
 * @brief 计算当前复合元组中所有元组的单元格总数。
 *
 * @return int 返回复合元组中单元格的数量。
 */
int CompositeTuple::cell_num() const
{
  int num = 0;                         // 初始化单元格数量为0
  for (const auto &tuple : tuples_) {  // 遍历所有子元组
    num += tuple->cell_num();          // 累加每个子元组的单元格数量
  }
  return num;  // 返回总单元格数量
}

/**
 * @brief 获取指定索引位置的单元格的值。
 *
 * @param index 要访问的单元格索引。
 * @param cell 存放单元格值的引用。
 * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
 */
RC CompositeTuple::cell_at(int index, Value &cell) const
{
  for (const auto &tuple : tuples_) {      // 遍历所有子元组
    if (index < tuple->cell_num()) {       // 如果索引在当前元组的范围内
      return tuple->cell_at(index, cell);  // 调用子元组的cell_at获取单元格值
    } else {
      index -= tuple->cell_num();  // 否则，减去当前元组的单元格数量，继续查找
    }
  }
  return RC::NOTFOUND;  // 如果没有找到，返回未找到状态
}

/**
 * @brief 获取指定索引位置的单元格规格。
 *
 * @param index 要访问的单元格索引。
 * @param spec 存放单元格规格的引用。
 * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
 */
RC CompositeTuple::spec_at(int index, TupleCellSpec &spec) const
{
  for (const auto &tuple : tuples_) {      // 遍历所有子元组
    if (index < tuple->cell_num()) {       // 如果索引在当前元组的范围内
      return tuple->spec_at(index, spec);  // 调用子元组的spec_at获取单元格规格
    } else {
      index -= tuple->cell_num();  // 否则，减去当前元组的单元格数量，继续查找
    }
  }
  return RC::NOTFOUND;  // 如果没有找到，返回未找到状态
}

/**
 * @brief 根据单元格规格查找相应的单元格值。
 *
 * @param spec 要查找的单元格规格。
 * @param cell 存放找到的单元格值的引用。
 * @return RC 返回操作的状态码，成功则返回RC::SUCCESS，否则返回RC::NOTFOUND。
 */
RC CompositeTuple::find_cell(const TupleCellSpec &spec, Value &cell) const
{
  RC rc = RC::NOTFOUND;                 // 初始化返回状态为未找到
  for (const auto &tuple : tuples_) {   // 遍历所有子元组
    rc = tuple->find_cell(spec, cell);  // 调用子元组的find_cell查找单元格
    if (OB_SUCC(rc)) {                  // 如果找到
      break;                            // 跳出循环
    }
  }
  return rc;  // 返回找到的状态
}

/**
 * @brief 添加一个子元组到复合元组中。
 *
 * @param tuple 要添加的子元组的唯一指针。
 */
void CompositeTuple::add_tuple(unique_ptr<Tuple> tuple)
{
  tuples_.push_back(std::move(tuple));  // 将子元组移动到复合元组的集合中
}

/**
 * @brief 获取指定索引位置的子元组。
 *
 * @param index 要访问的子元组索引。
 * @return Tuple& 返回指定索引位置的子元组的引用。
 */
Tuple &CompositeTuple::tuple_at(size_t index)
{
  ASSERT(index < tuples_.size(), "index=%d, tuples_size=%d", index, tuples_.size());  // 断言索引有效性
  return *tuples_[index];                                                        // 返回子元组的引用
}
