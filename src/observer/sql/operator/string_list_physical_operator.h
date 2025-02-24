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
// Created by WangYunlai on 2022/11/18.
//

#pragma once

#include "sql/operator/physical_operator.h"  // 引入物理算子基类
#include <vector>                            // 引入向量容器

/**
 * @brief 字符串列表物理算子
 * @ingroup PhysicalOperator
 * @details 用于将字符串列表转换为物理算子,为了方便实现的接口，比如help命令
 */
class StringListPhysicalOperator : public PhysicalOperator
{
public:
  StringListPhysicalOperator() {}  // 构造函数

  virtual ~StringListPhysicalOperator() = default;  // 默认析构函数

  // 向字符串列表中追加一段字符串
  template <typename InputIt>
  void append(InputIt begin, InputIt end)
  {
    strings_.emplace_back(begin, end);  // 将范围内的字符串添加到列表
  }

  // 追加初始化列表中的字符串
  void append(std::initializer_list<std::string> init) { strings_.emplace_back(init); }

  // 追加单个字符串
  template <typename T>
  void append(const T &v)
  {
    strings_.emplace_back(1, v);  // 将单个字符串添加到列表
  }

  // 获取物理算子类型
  PhysicalOperatorType type() const override { return PhysicalOperatorType::STRING_LIST; }

  // 打开物理算子，初始化状态
  RC open(Trx *) override { return RC::SUCCESS; }

  // 获取下一个结果
  RC next() override
  {
    if (!started_) {
      started_  = true;              // 第一次调用，标记为已启动
      iterator_ = strings_.begin();  // 获取字符串列表的开始迭代器
    } else if (iterator_ != strings_.end()) {
      ++iterator_;  // 移动到下一个元素
    }
    return iterator_ == strings_.end() ? RC::RECORD_EOF : RC::SUCCESS;  // 如果到达末尾返回 EOF
  }

  // 关闭物理算子，重置状态
  virtual RC close() override
  {
    iterator_ = strings_.end();  // 重置迭代器到列表末尾
    return RC::SUCCESS;          // 返回成功状态
  }

  // 获取当前元组
  virtual Tuple *current_tuple() override
  {
    if (iterator_ == strings_.end()) {
      return nullptr;  // 如果迭代器到达末尾，返回空
    }

    const StringList  &string_list = *iterator_;  // 获取当前字符串列表
    std::vector<Value> cells;                     // 用于存储元组单元

    // 遍历当前字符串列表并填充元组单元
    for (const std::string &s : string_list) {
      Value value(s.c_str());  // 将字符串转换为值
      cells.push_back(value);  // 添加到单元中
    }

    tuple_.set_cells(cells);  // 设置元组单元
    return &tuple_;           // 返回元组
  }

private:
  using StringList     = std::vector<std::string>;  // 字符串列表类型定义
  using StringListList = std::vector<StringList>;   // 字符串列表的列表类型定义
  StringListList           strings_;                // 字符串列表的容器
  StringListList::iterator iterator_;               // 迭代器
  bool                     started_ = false;        // 启动标识
  ValueListTuple           tuple_;                  // 存储当前元组的对象
};
