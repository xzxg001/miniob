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
// Created by Longda on 2021/4/14.
//

// 防止头文件重复包含
#pragma once

//! 该文档用于INI设置

// 客户端地址的配置项名称
#define CLIENT_ADDRESS "CLIENT_ADDRESS"

// 最大连接数的配置项名称
#define MAX_CONNECTION_NUM "MAX_CONNECTION_NUM"

// 默认最大连接数
#define MAX_CONNECTION_NUM_DEFAULT 8192

// 端口的配置项名称
#define PORT "PORT"

// 默认端口
#define PORT_DEFAULT 6789

// 套接字缓冲区大小
#define SOCKET_BUFFER_SIZE 8192

// 会话阶段的名称
#define SESSION_STAGE_NAME "SessionStage"
