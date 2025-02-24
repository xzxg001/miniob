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
// Created by Wangyunlai on 2023/6/14.
//

// 确保头文件只被包含一次
#pragma once

// 引入必要的头文件，这些头文件提供了类所需的支持
#include "common/rc.h"  // 提供返回码RC的定义
#include "event/session_event.h"  // 提供SessionEvent的定义
#include "event/sql_event.h"  // 提供SQLStageEvent的定义
#include "session/session.h"  // 提供Session的定义
#include "storage/trx/trx.h"  // 提供Trx的定义

/**
 * @brief 事务开始语句的执行器
 * @ingroup Executor
 */
class TrxBeginExecutor
{
public:
  // 默认构造函数
  TrxBeginExecutor()          = default;
  
  // 虚析构函数，确保派生类能正确析构
  virtual ~TrxBeginExecutor() = default;

  // execute函数用于执行事务开始操作
  RC execute(SQLStageEvent *sql_event)
  {
    // 从sql_event中获取SessionEvent对象
    SessionEvent *session_event = sql_event->session_event();

    // 从SessionEvent对象中获取Session对象和当前事务对象
    Session *session = session_event->session();
    Trx     *trx     = session->current_trx();

    // 设置会话为多操作事务模式
    session->set_trx_multi_operation_mode(true);

    // 调用事务对象的start_if_need方法，开始事务（如果尚未开始）
    return trx->start_if_need();
  }
};