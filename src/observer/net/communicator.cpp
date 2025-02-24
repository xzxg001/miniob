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
// Created by Wangyunlai on 2022/11/17.
//
// 引入必要的头文件
#include "net/communicator.h" // 定义了Communicator类
#include "net/buffered_writer.h" // 定义了BufferedWriter类，用于缓冲写操作
#include "net/cli_communicator.h" // 定义了CliCommunicator类，用于命令行界面通信
#include "net/mysql_communicator.h" // 定义了MysqlCommunicator类，用于MySQL协议通信
#include "net/plain_communicator.h" // 定义了PlainCommunicator类，用于普通文本通信
#include "session/session.h" // 定义了Session类，用于会话管理

#include "common/lang/mutex.h" // 引入互斥锁，用于线程同步

// Communicator类的初始化函数
// fd: 文件描述符，用于网络通信
// session: 会话对象，用于管理数据库会话
// addr: 通信对方的地址
RC Communicator::init(int fd, unique_ptr<Session> session, const std::string &addr)
{
  fd_      = fd; // 保存文件描述符
  session_ = std::move(session); // 移动会话对象，避免复制
  addr_    = addr; // 保存地址
  writer_  = new BufferedWriter(fd_); // 创建BufferedWriter对象，用于写操作
  return RC::SUCCESS; // 返回成功状态
}

// Communicator类的析构函数，用于资源清理
Communicator::~Communicator()
{
  if (fd_ >= 0) {
    close(fd_); // 关闭文件描述符
    fd_ = -1; // 将文件描述符设置为无效值
  }

  if (writer_ != nullptr) {
    delete writer_; // 删除BufferedWriter对象
    writer_ = nullptr; // 将指针设置为nullptr
  }
}

//////////////////////////////////////////////////////////////////////////////////

// CommunicatorFactory类的create函数，用于根据协议创建不同的Communicator对象
Communicator *CommunicatorFactory::create(CommunicateProtocol protocol)
{
  switch (protocol) {
    case CommunicateProtocol::PLAIN: {
      return new PlainCommunicator; // 创建PlainCommunicator对象
    } break;
    case CommunicateProtocol::CLI: {
      return new CliCommunicator; // 创建CliCommunicator对象
    } break;
    case CommunicateProtocol::MYSQL: {
      return new MysqlCommunicator; // 创建MysqlCommunicator对象
    } break;
    default: {
      return nullptr; // 如果协议不支持，返回nullptr
    }
  }
}