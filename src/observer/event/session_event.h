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

#pragma once  // 防止头文件被多次包含

#include "common/lang/string.h"       // 引入字符串处理相关的头文件
#include "event/sql_debug.h"          // 引入 SQL 调试相关的头文件
#include "sql/executor/sql_result.h"  // 引入 SQL 执行结果相关的头文件

// 前向声明，减少头文件之间的依赖
class Session;       // 会话类的前向声明
class Communicator;  // 通信类的前向声明

/**
 * @brief 表示一个 SQL 请求的类
 *
 * 该类封装了 SQL 请求的执行上下文，包括与客户端的通信、执行结果和调试信息。
 */
class SessionEvent
{
public:
  /**
   * @brief SessionEvent 构造函数
   *
   * @param client 指向 Communicator 对象的指针，用于与客户端进行通信。
   */
  SessionEvent(Communicator *client);

  /**
   * @brief SessionEvent 析构函数
   *
   * 析构函数，在对象被销毁时调用，用于释放资源。
   */
  virtual ~SessionEvent();

  /**
   * @brief 获取与此事件关联的 Communicator 对象
   *
   * @return 返回指向 Communicator 对象的指针。
   */
  Communicator *get_communicator() const;

  /**
   * @brief 获取当前会话
   *
   * @return 返回指向 Session 对象的指针。
   */
  Session *session() const;

  /**
   * @brief 设置 SQL 查询字符串
   *
   * @param query 要设置的 SQL 查询字符串。
   */
  void set_query(const string &query) { query_ = query; }

  /**
   * @brief 获取当前的 SQL 查询字符串
   *
   * @return 返回指向当前 SQL 查询字符串的引用。
   */
  const string &query() const { return query_; }

  /**
   * @brief 获取 SQL 执行结果
   *
   * @return 返回指向 SqlResult 对象的指针。
   */
  SqlResult *sql_result() { return &sql_result_; }

  /**
   * @brief 获取 SQL 调试信息
   *
   * @return 返回指向 SqlDebug 对象的引用。
   */
  SqlDebug &sql_debug() { return sql_debug_; }

private:
  Communicator *communicator_ = nullptr;  ///< 与客户端通信的对象
  SqlResult     sql_result_;              ///< SQL 执行结果
  SqlDebug      sql_debug_;               ///< SQL 调试信息
  string        query_;                   ///< SQL 查询字符串
};
