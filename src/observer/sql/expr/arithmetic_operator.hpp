/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// 头文件保护，防止重复包含
#pragma once

// 如果使用了SIMD优化，包含SIMD相关的头文件
#if defined(USE_SIMD)
#include "common/math/simd_util.h"
#endif

// 包含Column相关的定义
#include "storage/common/column.h"

// 定义Equal结构体，用于相等比较操作
struct Equal
{
  // 模板函数，比较两个普通数据类型的相等性
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left == right;  // 返回两个操作数是否相等
  }

  // 如果定义了SIMD优化，添加SIMD浮点数相等比较函数
#if defined(USE_SIMD)
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_EQ_OS);  // 使用SIMD比较浮点数是否相等
  }

  // SIMD整数相等比较函数
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    return _mm256_cmpeq_epi32(left, right);  // 使用SIMD比较整数是否相等
  }
#endif
};

// 定义NotEqual结构体，用于不等比较操作
struct NotEqual
{
  // 模板函数，比较两个普通数据类型的不等性
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left != right;  // 返回两个操作数是否不相等
  }

  // 如果定义了SIMD优化，添加SIMD浮点数不等比较函数
#if defined(USE_SIMD)
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_NEQ_OS);  // 使用SIMD比较浮点数是否不等
  }

  // SIMD整数不等比较函数
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    // 先通过SIMD比较相等，然后通过位异或操作取反得到不相等结果
    return _mm256_xor_si256(_mm256_set1_epi32(-1), _mm256_cmpeq_epi32(left, right));
  }
#endif
};

// 定义GreatThan结构体，用于大于比较操作
struct GreatThan
{
  // 模板函数，比较两个普通数据类型的大小
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left > right;  // 返回left是否大于right
  }

  // 如果定义了SIMD优化，添加SIMD浮点数大于比较函数
#if defined(USE_SIMD)
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_GT_OS);  // 使用SIMD比较浮点数是否大于
  }

  // SIMD整数大于比较函数
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    return _mm256_cmpgt_epi32(left, right);  // 使用SIMD比较整数是否大于
  }
#endif
};

// 定义GreatEqual结构体，用于大于等于比较操作
struct GreatEqual
{
  // 模板函数，比较两个普通数据类型的大小关系（大于等于）
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left >= right;  // 返回left是否大于等于right
  }

  // 如果定义了SIMD优化，添加SIMD浮点数大于等于比较函数
#if defined(USE_SIMD)
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_GE_OS);  // 使用SIMD比较浮点数是否大于等于
  }

  // SIMD整数大于等于比较函数
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    // 先通过SIMD比较大于，再通过相等比较结果合并得到大于等于结果
    return _mm256_cmpgt_epi32(left, right) | _mm256_cmpeq_epi32(left, right);
  }
#endif
};

// 定义LessThan结构体，用于小于比较操作
struct LessThan
{
  // 模板函数，比较两个普通数据类型的大小
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left < right;  // 返回left是否小于right
  }

#if defined(USE_SIMD)
  // 添加SIMD浮点数小于比较函数
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_LT_OS);  // 使用SIMD比较浮点数是否小于
  }

  // SIMD整数小于比较函数
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    return _mm256_cmpgt_epi32(right, left);  // 使用SIMD比较整数是否小于（反转操作数位置）
  }
#endif
};

struct LessEqual
{
  /**
   * @brief 模板函数，用于比较两个元素是否小于等于
   * @tparam T 类型参数，可以是任意类型
   * @param left 左操作数
   * @param right 右操作数
   * @return bool 返回比较结果，left <= right
   */
  template <class T>
  static inline bool operation(const T &left, const T &right)
  {
    return left <= right;  // 判断左操作数是否小于等于右操作数
  }

#if defined(USE_SIMD)
  /**
   * @brief 使用SIMD指令比较两个256位浮点向量是否小于等于
   * @param left 左操作数，256位浮点向量
   * @param right 右操作数，256位浮点向量
   * @return __m256 返回256位浮点向量，每个元素表示比较结果
   */
  static inline __m256 operation(const __m256 &left, const __m256 &right)
  {
    return _mm256_cmp_ps(left, right, _CMP_LE_OS);  // 使用AVX指令进行浮点数比较（小于等于）
  }

  /**
   * @brief 使用SIMD指令比较两个256位整数向量是否小于等于
   * @param left 左操作数，256位整数向量
   * @param right 右操作数，256位整数向量
   * @return __m256i 返回256位整数向量，每个元素表示比较结果
   */
  static inline __m256i operation(const __m256i &left, const __m256i &right)
  {
    // 使用SIMD指令实现整数小于等于的逻辑：left <= right 等价于 right > left 或者 left == right
    return _mm256_or_si256(_mm256_cmpgt_epi32(right, left), _mm256_cmpeq_epi32(left, right));
  }
#endif
};

struct AddOperator
{
  /**
   * @brief 模板函数，用于对两个元素进行加法运算
   * @tparam T 类型参数，可以是任意类型
   * @param left 左操作数
   * @param right 右操作数
   * @return T 返回加法结果，left + right
   */
  template <class T>
  static inline T operation(T left, T right)
  {
    return left + right;  // 返回两个操作数相加的结果
  }

#if defined(USE_SIMD)
  /**
   * @brief 使用SIMD指令对两个256位浮点向量进行加法运算
   * @param left 左操作数，256位浮点向量
   * @param right 右操作数，256位浮点向量
   * @return __m256 返回加法结果的256位浮点向量
   */
  static inline __m256 operation(__m256 left, __m256 right)
  {
    return _mm256_add_ps(left, right);
  }  // 使用AVX指令进行加法运算

  /**
   * @brief 使用SIMD指令对两个256位整数向量进行加法运算
   * @param left 左操作数，256位整数向量
   * @param right 右操作数，256位整数向量
   * @return __m256i 返回加法结果的256位整数向量
   */
  static inline __m256i operation(__m256i left, __m256i right)
  {
    return _mm256_add_epi32(left, right);
  }  // 使用AVX指令进行整数加法运算
#endif
};

struct SubtractOperator
{
  /**
   * @brief 模板函数，用于对两个元素进行减法运算
   * @tparam T 类型参数，可以是任意类型
   * @param left 左操作数
   * @param right 右操作数
   * @return T 返回减法结果，left - right
   */
  template <class T>
  static inline T operation(T left, T right)
  {
    return left - right;  // 返回两个操作数相减的结果
  }

#if defined(USE_SIMD)
  /**
   * @brief 使用SIMD指令对两个256位浮点向量进行减法运算
   * @param left 左操作数，256位浮点向量
   * @param right 右操作数，256位浮点向量
   * @return __m256 由于未实现，调用时程序退出
   */
  static inline __m256 operation(__m256 left, __m256 right) { exit(-1); }  // 退出程序，未实现

  /**
   * @brief 使用SIMD指令对两个256位整数向量进行减法运算
   * @param left 左操作数，256位整数向量
   * @param right 右操作数，256位整数向量
   * @return __m256i 由于未实现，调用时程序退出
   */
  static inline __m256i operation(__m256i left, __m256i right) { exit(-1); }  // 退出程序，未实现
#endif
};

struct MultiplyOperator
{
  /**
   * @brief 模板函数，用于对两个元素进行乘法运算
   * @tparam T 类型参数，可以是任意类型
   * @param left 左操作数
   * @param right 右操作数
   * @return T 返回乘法结果，left * right
   */
  template <class T>
  static inline T operation(T left, T right)
  {
    return left * right;  // 返回两个操作数相乘的结果
  }

#if defined(USE_SIMD)
  /**
   * @brief 使用SIMD指令对两个256位浮点向量进行乘法运算
   * @param left 左操作数，256位浮点向量
   * @param right 右操作数，256位浮点向量
   * @return __m256 由于未实现，调用时程序退出
   */
  static inline __m256 operation(__m256 left, __m256 right) { exit(-1); }  // 退出程序，未实现

  /**
   * @brief 使用SIMD指令对两个256位整数向量进行乘法运算
   * @param left 左操作数，256位整数向量
   * @param right 右操作数，256位整数向量
   * @return __m256i 由于未实现，调用时程序退出
   */
  static inline __m256i operation(__m256i left, __m256i right) { exit(-1); }  // 退出程序，未实现
#endif
};

struct DivideOperator
{
  /**
   * @brief 模板函数，用于对两个元素进行除法运算
   * @tparam T 类型参数，可以是任意类型
   * @param left 左操作数
   * @param right 右操作数
   * @return T 返回除法结果，left / right
   * @note 当 right 为 0 时行为未定义，需注意
   */
  template <class T>
  static inline T operation(T left, T right)
  {
    // 注意：当右操作数为0时，可能导致除零错误
    return left / right;  // 返回两个操作数相除的结果
  }

#if defined(USE_SIMD)
  /**
   * @brief 使用SIMD指令对两个256位浮点向量进行除法运算
   * @param left 左操作数，256位浮点向量
   * @param right 右操作数，256位浮点向量
   * @return __m256 返回除法结果的256位浮点向量
   */
  static inline __m256 operation(__m256 left, __m256 right)
  {
    return _mm256_div_ps(left, right);
  }  // 使用AVX指令进行浮点除法运算

  /**
   * @brief 使用SIMD指令对两个256位整数向量进行除法运算
   * @param left 左操作数，256位整数向量
   * @param right 右操作数，256位整数向量
   * @return __m256i 返回除法结果的256位整数向量，整数除法通过先转换为浮点数完成
   */
  static inline __m256i operation(__m256i left, __m256i right)
  {
    // 将整数转换为浮点数，执行浮点除法，然后再转换回整数
    __m256 left_float   = _mm256_cvtepi32_ps(left);                // 将256位整数向量转换为浮点数
    __m256 right_float  = _mm256_cvtepi32_ps(right);               // 将256位整数向量转换为浮点数
    __m256 result_float = _mm256_div_ps(left_float, right_float);  // 执行浮点除法
    return _mm256_cvttps_epi32(result_float);                      // 将浮点数结果转换为整数并返回
  }
#endif
};

struct NegateOperator
{
  /**
   * @brief 模板函数，用于对输入值进行取反操作
   * @tparam T 类型参数，可以是任意类型
   * @param input 输入值
   * @return T 返回取反后的结果，-input
   */
  template <class T>
  static inline T operation(T input)
  {
    return -input;  // 返回输入值的负数
  }
};

template <typename T, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, class OP>
void compare_operation(T *left, T *right, int n, std::vector<uint8_t> &result)
{
#if defined(USE_SIMD)
  // SIMD优化部分代码
  int i = 0;

  // 如果数据类型为float类型
  if constexpr (std::is_same<T, float>::value) {
    for (; i <= n - SIMD_WIDTH; i += SIMD_WIDTH) {
      __m256 left_value, right_value;

      // 如果左操作数为常量
      if constexpr (LEFT_CONSTANT) {
        left_value = _mm256_set1_ps(left[0]);  // 将left[0]复制到256位寄存器的所有位置
      } else {
        left_value = _mm256_loadu_ps(&left[i]);  // 加载左操作数的数据到寄存器中
      }

      // 如果右操作数为常量
      if constexpr (RIGHT_CONSTANT) {
        right_value = _mm256_set1_ps(right[0]);  // 将right[0]复制到256位寄存器的所有位置
      } else {
        right_value = _mm256_loadu_ps(&right[i]);  // 加载右操作数的数据到寄存器中
      }

      // 调用OP类的operation函数，进行浮点数的操作
      __m256 result_values = OP::operation(left_value, right_value);

      // 将浮点结果转换为整数类型（掩码）
      __m256i mask = _mm256_castps_si256(result_values);

      // 对每个元素逐一处理
      for (int j = 0; j < SIMD_WIDTH; j++) {
        result[i + j] &= mm256_extract_epi32_var_indx(mask, j) ? 1 : 0;  // 提取掩码并更新结果
      }
    }
  }
  // 如果数据类型为int类型
  else if constexpr (std::is_same<T, int>::value) {
    for (; i <= n - SIMD_WIDTH; i += SIMD_WIDTH) {
      __m256i left_value, right_value;

      // 如果左操作数为常量
      if (LEFT_CONSTANT) {
        left_value = _mm256_set1_epi32(left[0]);  // 将left[0]复制到256位寄存器的所有位置
      } else {
        left_value = _mm256_loadu_si256((__m256i *)&left[i]);  // 加载左操作数的数据到寄存器中
      }

      // 如果右操作数为常量
      if (RIGHT_CONSTANT) {
        right_value = _mm256_set1_epi32(right[0]);  // 将right[0]复制到256位寄存器的所有位置
      } else {
        right_value = _mm256_loadu_si256((__m256i *)&right[i]);  // 加载右操作数的数据到寄存器中
      }

      // 调用OP类的operation函数，进行整数的操作
      __m256i result_values = OP::operation(left_value, right_value);

      // 对每个元素逐一处理
      for (int j = 0; j < SIMD_WIDTH; j++) {
        result[i + j] &= mm256_extract_epi32_var_indx(result_values, j) ? 1 : 0;  // 提取结果并更新
      }
    }
  }

  // 处理剩余不能被SIMD处理的部分
  for (; i < n; i++) {
    auto &left_value  = left[LEFT_CONSTANT ? 0 : i];              // 左操作数：如果是常量，使用left[0]
    auto &right_value = right[RIGHT_CONSTANT ? 0 : i];            // 右操作数：如果是常量，使用right[0]
    result[i] &= OP::operation(left_value, right_value) ? 1 : 0;  // 调用操作并更新结果
  }

#else
  // 非SIMD部分代码
  for (int i = 0; i < n; i++) {
    auto &left_value  = left[LEFT_CONSTANT ? 0 : i];              // 左操作数：如果是常量，使用left[0]
    auto &right_value = right[RIGHT_CONSTANT ? 0 : i];            // 右操作数：如果是常量，使用right[0]
    result[i] &= OP::operation(left_value, right_value) ? 1 : 0;  // 调用操作并更新结果
  }
#endif
}

/**
 * @brief 对两个数组进行二元操作，支持SIMD优化
 *
 * @tparam LEFT_CONSTANT 如果为true，左操作数为常量
 * @tparam RIGHT_CONSTANT 如果为true，右操作数为常量
 * @tparam T 数据类型，可以是float或int
 * @tparam OP 操作类，必须实现静态operation方法
 * @param left_data 左操作数数组
 * @param right_data 右操作数数组
 * @param result_data 存储结果的数组
 * @param size 数组的大小
 */
template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT, typename T, class OP>
void binary_operator(T *left_data, T *right_data, T *result_data, int size)
{
#if defined(USE_SIMD)
  int i = 0;  // 记录当前处理到的元素位置

  // 如果数据类型为float
  if constexpr (std::is_same<T, float>::value) {
    // 使用SIMD处理对齐的数据块
    for (; i <= size - SIMD_WIDTH; i += SIMD_WIDTH) {
      __m256 left_value, right_value;

      // 判断左操作数是否是常量
      if (LEFT_CONSTANT) {
        left_value = _mm256_set1_ps(left_data[0]);  // 将左操作数的第一个元素广播到256位寄存器中
      } else {
        left_value = _mm256_loadu_ps(&left_data[i]);  // 从左操作数加载256位浮点数据
      }

      // 判断右操作数是否是常量
      if (RIGHT_CONSTANT) {
        right_value = _mm256_set1_ps(right_data[0]);  // 将右操作数的第一个元素广播到256位寄存器中
      } else {
        right_value = _mm256_loadu_ps(&right_data[i]);  // 从右操作数加载256位浮点数据
      }

      // 调用操作类的operation函数，执行操作
      __m256 result_value = OP::operation(left_value, right_value);

      // 将结果存储回result_data数组中
      _mm256_storeu_ps(&result_data[i], result_value);
    }
  }
  // 如果数据类型为int
  else if constexpr (std::is_same<T, int>::value) {
    // 使用SIMD处理对齐的数据块
    for (; i <= size - SIMD_WIDTH; i += SIMD_WIDTH) {
      __m256i left_value, right_value;

      // 判断左操作数是否是常量
      if (LEFT_CONSTANT) {
        left_value = _mm256_set1_epi32(left_data[0]);  // 将左操作数的第一个元素广播到256位寄存器中
      } else {
        left_value = _mm256_loadu_si256((const __m256i *)&left_data[i]);  // 从左操作数加载256位整型数据
      }

      // 判断右操作数是否是常量
      if (RIGHT_CONSTANT) {
        right_value = _mm256_set1_epi32(right_data[0]);  // 将右操作数的第一个元素广播到256位寄存器中
      } else {
        right_value = _mm256_loadu_si256((const __m256i *)&right_data[i]);  // 从右操作数加载256位整型数据
      }

      // 调用操作类的operation函数，执行操作
      __m256i result_value = OP::operation(left_value, right_value);

      // 将结果存储回result_data数组中
      _mm256_storeu_si256((__m256i *)&result_data[i], result_value);
    }
  }

  // 处理剩余未对齐的数据
  for (; i < size; i++) {
    auto &left_value  = left_data[LEFT_CONSTANT ? 0 : i];    // 如果左操作数是常量，使用第一个元素
    auto &right_value = right_data[RIGHT_CONSTANT ? 0 : i];  // 如果右操作数是常量，使用第一个元素
    result_data[i] = OP::template operation<T>(left_value, right_value);  // 调用操作类的operation函数，计算结果
  }
#else
  // 非SIMD处理逻辑，逐元素处理
  for (int i = 0; i < size; i++) {
    auto &left_value  = left_data[LEFT_CONSTANT ? 0 : i];    // 如果左操作数是常量，使用第一个元素
    auto &right_value = right_data[RIGHT_CONSTANT ? 0 : i];  // 如果右操作数是常量，使用第一个元素
    result_data[i] = OP::template operation<T>(left_value, right_value);  // 调用操作类的operation函数，计算结果
  }
#endif
}

/**
 * @brief 对数组进行一元操作，支持常量输入
 *
 * @tparam CONSTANT 如果为true，输入为常量
 * @tparam T 数据类型，可以是float或int
 * @tparam OP 操作类，必须实现静态operation方法
 * @param input 输入数组
 * @param result_data 存储结果的数组
 * @param size 数组的大小
 */
template <bool CONSTANT, typename T, class OP>
void unary_operator(T *input, T *result_data, int size)
{
  // 逐元素处理
  for (int i = 0; i < size; i++) {
    auto &value    = input[CONSTANT ? 0 : i];           // 如果输入是常量，使用第一个元素
    result_data[i] = OP::template operation<T>(value);  // 调用操作类的operation函数，计算结果
  }
}

/**
 * @brief 根据比较操作符对两个数组进行比较，并将结果存储在`result`向量中
 *
 * @tparam T 数据类型，可以是`float`或`int`
 * @tparam LEFT_CONSTANT 如果为`true`，表示左操作数为常量
 * @tparam RIGHT_CONSTANT 如果为`true`，表示右操作数为常量
 * @param left 左操作数数组
 * @param right 右操作数数组
 * @param n 数组的大小
 * @param result 存储比较结果的向量，`uint8_t`类型，表示布尔值
 * @param op 比较操作符，`CompOp`枚举类型
 */
template <typename T, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
void compare_result(T *left, T *right, int n, std::vector<uint8_t> &result, CompOp op)
{
  // 根据比较操作符执行相应的比较操作
  switch (op) {
    case CompOp::EQUAL_TO: {
      // 执行相等比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, Equal>(left, right, n, result);
      break;
    }
    case CompOp::NOT_EQUAL: {
      // 执行不等比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, NotEqual>(left, right, n, result);
      break;
    }
    case CompOp::GREAT_EQUAL: {
      // 执行大于等于比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, GreatEqual>(left, right, n, result);
      break;
    }
    case CompOp::GREAT_THAN: {
      // 执行大于比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, GreatThan>(left, right, n, result);
      break;
    }
    case CompOp::LESS_EQUAL: {
      // 执行小于等于比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, LessEqual>(left, right, n, result);
      break;
    }
    case CompOp::LESS_THAN: {
      // 执行小于比较
      compare_operation<T, LEFT_CONSTANT, RIGHT_CONSTANT, LessThan>(left, right, n, result);
      break;
    }
    default:
      // 未知操作符，不执行任何操作
      break;
  }
}
