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

// Created by Wangyunlai on 2023/6/29.

#pragma once

#include "common/lang/string.h"  // 引入字符串处理相关的头文件
#include "common/lang/list.h"    // 引入列表处理相关的头文件

/**
 * @brief SQL 调试信息类
 *
 * @details
 * 此类用于管理 SQL 调试信息。在运行 SQL 语句时，可以收集并存储调试信息，以便在执行结果时输出。
 * 当前实现将调试信息与会话绑定，允许在 SQL 语句执行过程中输出调试信息。
 * 注意：当前实现尚不支持与行数据的同步输出。
 */
class SqlDebug
{
public:
  SqlDebug()          = default;  ///< 构造函数，默认初始化
  virtual ~SqlDebug() = default;  ///< 析构函数，默认析构

  /**
   * @brief 向调试信息列表中添加调试信息
   *
   * @param debug_info 要添加的调试信息字符串
   */
  void add_debug_info(const string &debug_info);

  /**
   * @brief 清空当前存储的调试信息
   *
   * @details 此方法将删除所有已存储的调试信息，重置调试信息列表。
   */
  void clear_debug_info();

  /**
   * @brief 获取当前的调试信息列表
   *
   * @return 返回包含调试信息的列表的常量引用
   */
  const list<string> &get_debug_infos() const;

private:
  list<string> debug_infos_;  ///< 存储调试信息的列表
};

/**
 * @brief 增加 SQL 调试信息
 *
 * @details 在执行 SQL 语句时，可以调用此函数增加调试信息。
 * 如果当前上下文不在 SQL 执行过程中，则不会生成调试信息。
 * 调试信息会直接输出到客户端，并在文本前增加 '#' 作为前缀。
 *
 * @param fmt 格式化字符串，支持可变参数
 * @param ... 格式化字符串后面的可变参数
 */
void sql_debug(const char *fmt, ...);
