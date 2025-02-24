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

// Created by Longda on 2021/4/14.

#include "event/sql_event.h"      // 引入 SQL 事件相关的头文件
#include "event/session_event.h"  // 引入会话事件相关的头文件
#include "sql/stmt/stmt.h"        // 引入 SQL 语句相关的头文件

/**
 * @brief SQL 阶段事件类
 *
 * @details
 * 此类表示一个 SQL 执行阶段的事件。它包含与当前会话事件相关的信息
 * 以及正在执行的 SQL 语句。该类的主要功能是管理 SQL 执行阶段的状态。
 */
class SQLStageEvent
{
public:
  /**
   * @brief 构造函数
   *
   * @param event 与此 SQL 阶段事件相关联的会话事件指针
   * @param sql 要执行的 SQL 语句
   */
  SQLStageEvent(SessionEvent *event, const string &sql);

  /**
   * @brief 析构函数
   *
   * @details 在析构时释放与会话事件和 SQL 语句相关的资源。
   * 确保在销毁对象时，所有动态分配的内存都被正确释放。
   */
  virtual ~SQLStageEvent() noexcept;

private:
  SessionEvent *session_event_;   ///< 指向相关的会话事件对象的指针
  string        sql_;             ///< 要执行的 SQL 语句
  Stmt         *stmt_ = nullptr;  ///< 指向 SQL 语句对象的指针，用于管理 SQL 语句的执行
};

// SQLStageEvent 的构造函数实现
SQLStageEvent::SQLStageEvent(SessionEvent *event, const string &sql) : session_event_(event), sql_(sql) {}

// SQLStageEvent 的析构函数实现
SQLStageEvent::~SQLStageEvent() noexcept
{
  if (session_event_ != nullptr) {
    session_event_ = nullptr;  // 清空与会话事件的指针
  }

  if (stmt_ != nullptr) {
    delete stmt_;     // 释放动态分配的 SQL 语句对象
    stmt_ = nullptr;  // 清空 SQL 语句对象的指针
  }
}
