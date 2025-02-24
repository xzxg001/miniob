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
// Created by Longda on 2021/5/3.
//

#pragma once  // 确保该文件只被包含一次，防止重复定义

#include "common/conf/ini.h"          // 引入 INI 配置文件处理的头文件
#include "common/os/process_param.h"  // 引入与进程参数相关的头文件

/**
 * @brief 初始化函数
 * @param processParam 进程参数的指针，包含初始化所需的参数
 * @return 返回整数值，通常用于指示初始化的成功或失败
 */
int init(common::ProcessParam *processParam);

/**
 * @brief 清理函数
 * 释放或清理在初始化过程中分配的资源
 */
void cleanup();
