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
// Created by Wangyunlai on 2024/05/30.
//

#pragma once

#include <functional>  // 引入函数对象和 std::function
#include <memory>      // 引入智能指针

#include "common/rc.h"  // 引入返回码类型定义

class Expression;  // 前向声明 Expression 类

/**
 * @brief 表达式迭代器
 *
 * 该类用于遍历表达式树的子表达式，允许用户通过回调函数处理每个子表达式。
 */
class ExpressionIterator
{
public:
  /**
   * @brief 遍历给定表达式的子表达式
   *
   * @param expr 要遍历的表达式
   * @param callback 处理每个子表达式的回调函数。接收一个唯一指针类型的子表达式作为参数，并返回操作结果的状态码。
   *
   * @return RC 操作的状态码，成功返回 RC::SUCCESS，失败返回相应的错误码
   */
  static RC iterate_child_expr(Expression &expr, std::function<RC(std::unique_ptr<Expression> &)> callback);
};
