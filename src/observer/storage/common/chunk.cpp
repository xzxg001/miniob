/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/common/chunk.h"

// 向 Chunk 中添加列
void Chunk::add_column(unique_ptr<Column> col, int col_id)
{
  columns_.push_back(std::move(col)); // 将列移动到 columns_ 向量中
  column_ids_.push_back(col_id); // 存储列的 ID
}

// 引用另一个 Chunk 的数据
RC Chunk::reference(Chunk &chunk)
{
  reset(); // 重置当前 Chunk
  this->columns_.resize(chunk.column_num()); // 调整当前 Chunk 的列数

  // 为每一列设置引用
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (nullptr == columns_[i]) {
      columns_[i] = make_unique<Column>(); // 如果当前列为空，创建新的 Column
    }
    columns_[i]->reference(chunk.column(i)); // 设置当前列引用外部 Chunk 的列
    column_ids_.push_back(chunk.column_ids(i)); // 存储外部 Chunk 的列 ID
  }
  return RC::SUCCESS; // 返回成功
}

// 获取 Chunk 的行数
int Chunk::rows() const
{
  if (!columns_.empty()) {
    return columns_[0]->count(); // 返回第一列的行数
  }
  return 0; // 如果没有列，则返回 0
}

// 获取 Chunk 的容量
int Chunk::capacity() const
{
  if (!columns_.empty()) {
    return columns_[0]->capacity(); // 返回第一列的容量
  }
  return 0; // 如果没有列，则返回 0
}

// 重置 Chunk 中每列的数据
void Chunk::reset_data()
{
  for (auto &col : columns_) {
    col->reset_data(); // 重置当前列的数据
  }
}

// 重置 Chunk，清空所有列和列 ID
void Chunk::reset()
{
  columns_.clear(); // 清空列向量
  column_ids_.clear(); // 清空列 ID 向量
}
