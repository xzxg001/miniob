/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <type_traits>  // 引入类型特征库以支持模板特性

#include "sql/expr/aggregate_state.h"  // 引入聚合状态定义头文件

#ifdef USE_SIMD
#include "common/math/simd_util.h"  // 引入 SIMD 实用工具头文件
#endif

/**
 * @brief 用于计算求和的状态类。
 * @tparam T 聚合值的数据类型，支持 int 和 float。
 */
template <typename T>
class SumState
{
public:
  T value;  // 存储当前的求和结果

  /**
   * @brief 更新求和状态。
   * @param values 指向要累加的值数组
   * @param size 要累加的值的数量
   */
  void update(const T *values, int size);
};

/**
 * @brief 更新求和状态的方法实现。
 * @param values 指向要累加的值数组
 * @param size 要累加的值的数量
 */
template <typename T>
void SumState<T>::update(const T *values, int size)
{
#ifdef USE_SIMD
  // 如果启用 SIMD，使用不同的加法实现
  if constexpr (std::is_same<T, float>::value) {       // 检查 T 是否为 float 类型
    value += mm256_sum_ps(values, size);               // 使用 AVX2 指令对 float 进行求和
  } else if constexpr (std::is_same<T, int>::value) {  // 检查 T 是否为 int 类型
    value += mm256_sum_epi32(values, size);            // 使用 AVX2 指令对 int 进行求和
  }
#else
  // 如果未启用 SIMD，则使用普通的循环进行求和
  for (int i = 0; i < size; ++i) {
    value += values[i];  // 累加每一个值
  }
#endif
}

// 显式实例化 SumState 模板，以便为 int 和 float 类型生成代码
template class SumState<int>;
template class SumState<float>;
