/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/**
 * @brief 聚合状态类，用于计算总和。
 *
 * @tparam T 聚合值的数据类型，可以是 int、float 等。
 */
template <class T>
class SumState
{
public:
  /**
   * @brief 构造函数，初始化聚合值为 0。
   */
  SumState() : value(0) {}

  T value;  // 当前的总和值

  /**
   * @brief 更新聚合状态，累加给定值数组中的所有值。
   *
   * @param values 指向要累加的值数组
   * @param size 数组中值的数量
   */
  void update(const T *values, int size);
};
