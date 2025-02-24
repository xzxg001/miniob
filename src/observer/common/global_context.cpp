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
// Created by Wangyunlai on 2023/5/29.
//

#include "common/global_context.h"  // 引入全局上下文的头文件

// 创建全局上下文的静态实例
static GlobalContext global_context;

/**
 * @brief 获取全局上下文的单例实例
 *
 * @return GlobalContext& 返回全局上下文的引用
 */
GlobalContext &GlobalContext::instance()
{
  return global_context;  // 返回全局上下文实例
}
