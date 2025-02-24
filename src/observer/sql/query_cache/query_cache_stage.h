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
// Created by Longda on 2021/4/13.
//

// 确保头文件只被包含一次
#pragma once

// 引入必要的头文件，提供返回码RC的定义
#include "common/rc.h"

// 声明SQLStageEvent类，表示SQL执行阶段的事件
class SQLStageEvent;

/**
 * @brief 查询缓存处理类
 * @ingroup SQLStage
 * @details 当前实现什么都没做，暗示这个类是查询缓存处理的占位符或框架
 */
class QueryCacheStage
{
public:
  // 默认构造函数
  QueryCacheStage()          = default;
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~QueryCacheStage() = default;

public:
  // handle_request函数用于处理SQL查询缓存请求
  RC handle_request(SQLStageEvent *sql_event);
};