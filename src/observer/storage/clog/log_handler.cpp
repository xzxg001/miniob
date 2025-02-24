/* Copyright (c) 2021-2022 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2024/02/01
//

#include <string.h>

#include "storage/clog/log_handler.h"
#include "common/lang/string.h"
#include "storage/clog/disk_log_handler.h"
#include "storage/clog/vacuous_log_handler.h"

// 将日志数据追加到日志处理器
RC LogHandler::append(LSN &lsn, LogModule::Id module, span<const char> data)
{
  vector<char> data_vec(data.begin(), data.end()); // 将输入数据转换为 vector<char>
  return append(lsn, module, std::move(data_vec)); // 调用另一个重载的 append 函数
}

// 将日志数据追加到日志处理器，使用右值引用
RC LogHandler::append(LSN &lsn, LogModule::Id module, vector<char> &&data)
{
  return _append(lsn, LogModule(module), std::move(data)); // 调用内部的 _append 函数
}

// 创建日志处理器实例
RC LogHandler::create(const char *name, LogHandler *&log_handler)
{
  if (name == nullptr || common::is_blank(name)) {
    name = "vacuous"; // 如果名称为空或空白，默认使用 "vacuous"
  }

  // 根据名称创建不同类型的日志处理器
  if (strcasecmp(name, "disk") == 0) {
    log_handler = new DiskLogHandler(); // 创建磁盘日志处理器
  } else if (strcasecmp(name, "vacuous") == 0) {
    log_handler = new VacuousLogHandler(); // 创建无效日志处理器
  } else {
    return RC::INVALID_ARGUMENT; // 如果名称不合法，返回无效参数错误
  }
  return RC::SUCCESS; // 返回成功
}
