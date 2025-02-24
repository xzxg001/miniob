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

#pragma once

class Trx;
class Session;

class ThreadData
{
public:
/**
 * 获取当前线程的数据对象
 * 
 * 此函数用于获取当前线程的线程数据对象指针它是一个静态方法，意味着它属于类本身，
 * 而不是类的实例通过这种方式，可以在不创建类实例的情况下访问线程数据
 * 
 * @return ThreadData* 返回当前线程的线程数据对象指针如果未初始化或不可用，则可能返回nullptr
 */
static ThreadData *current() { return thread_data_; }
/**
 * 设置线程数据
 * 
 * @param thread 线程数据指针，用于在线程中传递和存储特定的数据
 * 
 * 此函数用于初始化或更新线程相关的数据它接受一个指向ThreadData对象的指针，
 * 并将其赋值给全局或类级别的thread_data_变量这种设计模式允许在整个程序或类中共享线程特定的数据
 */
static void setup(ThreadData *thread) { thread_data_ = thread; }

public:
// 默认构造函数，用于创建线程数据对象
ThreadData()  = default;
// 默认析构函数
~ThreadData() = default;

/**
 * 获取当前的会话对象
 * 
 * 会话对象用于管理与当前操作相关的一系列数据和状态
 * 通过此函数可以访问或操作会话中的数据
 * 
 * @return Session* 返回指向会话对象的指针
 */
Session *session() const { return session_; }
  Trx     *trx() const;

/**
 * 设置会话
 * 
 * @param session 指向会话对象的指针，用于在当前上下文中关联一个会话
 */
void set_session(Session *session) { session_ = session; }

private:
  static thread_local ThreadData *thread_data_;

private:
  Session *session_ = nullptr;
};