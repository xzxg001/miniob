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

#pragma once

class BufferPoolManager;  // 前向声明 BufferPoolManager 类
class DefaultHandler;     // 前向声明 DefaultHandler 类
class TrxKit;             // 前向声明 TrxKit 类

/**
 * @brief 全局上下文结构体
 *
 * @details 此结构体用于封装全局对象，以便更好地管理和访问。
 *          初始化的过程可以参考 init_global_objects 函数。
 */
struct GlobalContext
{
  // BufferPoolManager *buffer_pool_manager_ = nullptr; // 缓冲池管理器指针（未启用）
  DefaultHandler *handler_ = nullptr;  // 默认处理器指针
  // TrxKit            *trx_kit_             = nullptr; // 事务处理工具指针（未启用）

  /**
   * @brief 获取全局上下文的单例实例
   *
   * @return GlobalContext& 全局上下文的引用
   */
  static GlobalContext &instance();
};

// 定义全局上下文的实例化
#define GCTX GlobalContext::instance()
