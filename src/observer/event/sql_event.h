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

#pragma once

#include "common/lang/string.h"              // 引入字符串处理相关的头文件
#include "common/lang/memory.h"              // 引入内存管理相关的头文件
#include "sql/operator/physical_operator.h"  // 引入物理操作符相关的头文件

class SessionEvent;   // 前向声明 SessionEvent 类
class Stmt;           // 前向声明 Stmt 类
class ParsedSqlNode;  // 前向声明 ParsedSqlNode 类

/**
 * @brief SQL阶段事件类
 *
 * @details
 * 此类与 SessionEvent 类似，用于处理 SQL 请求的事件。
 * 但是，它在 SQL 的不同阶段使用，负责管理 SQL 执行的各个阶段。
 */
class SQLStageEvent
{
public:
  /**
   * @brief 构造函数
   *
   * @param event 与此 SQL 阶段事件关联的会话事件指针
   * @param sql 要处理的 SQL 语句
   */
  SQLStageEvent(SessionEvent *event, const string &sql);

  /**
   * @brief 析构函数
   *
   * @details 在析构时清理与会话事件、SQL 语句和其他相关资源。
   */
  virtual ~SQLStageEvent() noexcept;

  /**
   * @brief 获取关联的会话事件
   *
   * @return 返回指向 SessionEvent 对象的指针
   */
  SessionEvent *session_event() const { return session_event_; }

  /**
   * @brief 获取处理的 SQL 语句
   *
   * @return 返回 SQL 语句字符串的常量引用
   */
  const string &sql() const { return sql_; }

  /**
   * @brief 获取解析后的 SQL 节点
   *
   * @return 返回解析后的 SQL 节点的智能指针常量引用
   */
  const unique_ptr<ParsedSqlNode> &sql_node() const { return sql_node_; }

  /**
   * @brief 获取 SQL 语句对应的 Stmt 对象
   *
   * @return 返回指向 Stmt 对象的指针
   */
  Stmt *stmt() const { return stmt_; }

  /**
   * @brief 获取物理操作符
   *
   * @return 返回物理操作符的智能指针的引用
   */
  unique_ptr<PhysicalOperator> &physical_operator() { return operator_; }

  /**
   * @brief 获取物理操作符（常量版本）
   *
   * @return 返回物理操作符的智能指针的常量引用
   */
  const unique_ptr<PhysicalOperator> &physical_operator() const { return operator_; }

  /**
   * @brief 设置 SQL 语句
   *
   * @param sql 要设置的 SQL 语句字符串
   */
  void set_sql(const char *sql) { sql_ = sql; }

  /**
   * @brief 设置解析后的 SQL 节点
   *
   * @param sql_node 解析后的 SQL 节点的智能指针
   */
  void set_sql_node(unique_ptr<ParsedSqlNode> sql_node) { sql_node_ = std::move(sql_node); }

  /**
   * @brief 设置 SQL 语句对应的 Stmt 对象
   *
   * @param stmt 指向 Stmt 对象的指针
   */
  void set_stmt(Stmt *stmt) { stmt_ = stmt; }

  /**
   * @brief 设置物理操作符
   *
   * @param oper 物理操作符的智能指针
   */
  void set_operator(unique_ptr<PhysicalOperator> oper) { operator_ = std::move(oper); }

private:
  SessionEvent                *session_event_ = nullptr;  ///< 关联的会话事件指针
  string                       sql_;                      ///< 处理的 SQL 语句
  unique_ptr<ParsedSqlNode>    sql_node_;                 ///< 解析后的 SQL 命令
  Stmt                        *stmt_ = nullptr;           ///< 解析后生成的 SQL 语句数据结构
  unique_ptr<PhysicalOperator> operator_;                 ///< 生成的物理执行计划
};
