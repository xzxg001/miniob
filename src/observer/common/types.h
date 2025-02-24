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
// Created by Wangyunlai on 2022/6/23.
//

#pragma once  // 防止头文件被重复包含

#include <inttypes.h>  // 包含整数类型的定义
#include <stdint.h>    // 包含固定宽度整数类型的定义

/// 磁盘文件，包括存放数据的文件和索引(B+-Tree)文件，都按照页来组织
/// 每一页都有一个编号，称为 PageNum
using PageNum = int32_t;  // 页编号，使用 32 位有符号整数表示

/// 数据文件中按照页来组织，每一页会存放一些行数据(row)，或称为记录(record)
/// 每一行(row/record)，都占用一个槽位(slot)，这些槽有一个编号，称为 SlotNum
using SlotNum = int32_t;  // 槽位编号，使用 32 位有符号整数表示

/// LSN for log sequence number
using LSN = int64_t;  // 日志序列号，使用 64 位有符号整数表示

#define LSN_FORMAT PRId64  // 定义 LSN 的格式，用于格式化输出

/**
 * @brief 读写模式
 * @details 原来的代码中有大量的 true/false 来表示是否只读，这种代码不易于阅读
 */
enum class ReadWriteMode
{
  READ_ONLY,  // 只读模式
  READ_WRITE  // 读写模式
};

/**
 * @brief 存储格式
 * @details 当前仅支持行存格式（ROW_FORMAT）以及 PAX 存储格式（PAX_FORMAT）。
 */
enum class StorageFormat
{
  UNKNOWN_FORMAT = 0,  // 未知格式
  ROW_FORMAT,          // 行存格式
  PAX_FORMAT           // PAX 存储格式
};

/**
 * @brief 执行引擎模式
 * @details 当前支持按行处理（TUPLE_ITERATOR）以及按批处理（CHUNK_ITERATOR）两种模式。
 */
enum class ExecutionMode
{
  UNKNOWN_MODE = 0,  // 未知模式
  TUPLE_ITERATOR,    // 按行处理模式
  CHUNK_ITERATOR     // 按批处理模式
};

/// page 的 CRC 校验和
using CheckSum = unsigned int;  // CRC 校验和，使用无符号整数表示
