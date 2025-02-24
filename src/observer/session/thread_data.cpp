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
// Created by Wangyunlai on 2023/03/07.
//

#include "session/thread_data.h"
#include "session/session.h"

thread_local ThreadData *ThreadData::thread_data_;

/**
 * 获取当前线程的事务对象
 * 
 * 此函数尝试获取当前线程所关联的会话对象中的当前事务如果线程未关联任何会话，
 * 或者会话中不存在活动事务，则返回空指针这一设计允许调用者快速判断当前线程是否
 * 有活动的事务，而无需先检查会话是否存在，再检查事务
 * 
 * @return Trx* 返回指向当前事务的指针，如果没有事务则返回nullptr
 */
Trx *ThreadData::trx() const { return (session_ == nullptr) ? nullptr : session_->current_trx(); }