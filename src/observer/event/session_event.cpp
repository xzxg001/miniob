/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
您可以根据 Mulan PSL v2 的条款和条件使用本软件。
您可以在以下网址获取 Mulan PSL v2 的副本：
         http://license.coscl.org.cn/MulanPSL2
本软件是按“原样”提供的，不提供任何类型的保证，
无论是明示还是暗示，包括但不限于不侵权、
适销性或特定用途的适用性。
有关详细信息，请参阅 Mulan PSL v2。
*/
// Created by Longda on 2021/4/13.
#include "session_event.h"     // 引入 SessionEvent 类的头文件
#include "net/communicator.h"  // 引入 Communicator 类的头文件

/**
 * @brief SessionEvent 类的构造函数
 *
 * @param comm 指向 Communicator 对象的指针，用于与会话进行通信。
 * 初始化 communicator_ 成员并获取会话的 SQL 结果。
 */
SessionEvent::SessionEvent(Communicator *comm)
    : communicator_(comm), sql_result_(communicator_->session())  // 使用 Communicator 获取 Session
{}

/**
 * @brief SessionEvent 类的析构函数
 *
 * 析构函数，释放 SessionEvent 对象时调用。
 */
SessionEvent::~SessionEvent() {}

/**
 * @brief 获取关联的 Communicator 对象
 *
 * @return 返回指向 Communicator 对象的指针。
 */
Communicator *SessionEvent::get_communicator() const { return communicator_; }

/**
 * @brief 获取与该事件相关联的 Session 对象
 *
 * @return 返回指向 Session 对象的指针。
 */
Session *SessionEvent::session() const
{
  return communicator_->session();  // 通过 Communicator 获取当前 Session
}
