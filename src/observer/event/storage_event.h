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
//
// Created by Longda on 2021/4/14.
//

#pragma once

#include "common/seda/stage_event.h"  // 引入 SEDA 阶段事件相关的头文件

class SQLStageEvent;  // 前向声明 SQLStageEvent 类

/**
 * @brief 存储事件类
 *
 * @details
 * 此类继承自 common::StageEvent，用于表示与 SQL 执行相关的存储事件。
 * 它主要用于处理与 SQL 存储阶段相关的事件，并持有一个指向 SQLStageEvent 的指针。
 */
class StorageEvent : public common::StageEvent
{
public:
  /**
   * @brief 构造函数
   *
   * @param sql_event 指向与此存储事件相关的 SQL 阶段事件的指针
   */
  StorageEvent(SQLStageEvent *sql_event) : sql_event_(sql_event) {}

  /**
   * @brief 析构函数
   *
   * @details
   * 析构函数在对象销毁时进行清理操作。由于 SQLStageEvent 是一个指针类型，
   * 因此在此类中不需要显式删除 sql_event_ 指针，以免造成双重释放。
   */
  virtual ~StorageEvent();

  /**
   * @brief 获取与此存储事件关联的 SQL 阶段事件
   *
   * @return 返回指向 SQLStageEvent 对象的指针
   */
  SQLStageEvent *sql_event() const { return sql_event_; }

private:
  SQLStageEvent *sql_event_;  ///< 与此存储事件相关的 SQL 阶段事件指针
};
