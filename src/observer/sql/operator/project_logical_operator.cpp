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
// Created by Wangyunlai on 2022/12/15
//

#include "sql/operator/project_logical_operator.h"  // 包含ProjectLogicalOperator类的定义

using namespace std;  // 使用标准命名空间

/**
 * @brief ProjectLogicalOperator 构造函数
 * @param expressions 用于投影的表达式列表
 * @details 此构造函数将接收到的表达式移动到类的内部成员中，完成逻辑算子的初始化。
 */
ProjectLogicalOperator::ProjectLogicalOperator(vector<unique_ptr<Expression>> &&expressions)
{
  // 将输入的表达式列表移动到成员变量 expressions_ 中
  expressions_ = std::move(expressions);
}
