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
// Created by Wangyunlai on 2023/06/25.
//
#pragma once

#include "net/communicator.h"  // 引入Communicator基类
#include "common/lang/vector.h"  // 引入vector容器

class SqlResult;  // 前向声明SqlResult类，表示SQL执行结果

/**
 * @brief 与客户端进行通讯
 * @ingroup Communicator
 * @details 使用简单的文本通讯协议，每个消息使用'\0'结尾
 */
class PlainCommunicator : public Communicator  // 继承自Communicator基类
{
public:
  PlainCommunicator();  // 构造函数
  virtual ~PlainCommunicator() = default;  // 虚析构函数，允许派生类重写析构函数

  // 从网络读取事件，读取的数据将封装到SessionEvent对象中
  RC read_event(SessionEvent *&event) override;

  // 将结果写回给客户端
  RC write_result(SessionEvent *event, bool &need_disconnect) override;

private:
  // 将执行状态写回给客户端
  RC write_state(SessionEvent *event, bool &need_disconnect);

  // 将调试信息写回给客户端
  RC write_debug(SessionEvent *event, bool &need_disconnect);

  // 内部方法，用于写入结果集
  RC write_result_internal(SessionEvent *event, bool &need_disconnect);

  // 将Tuple结果集写回给客户端
  RC write_tuple_result(SqlResult *sql_result);

  // 将Chunk结果集写回给客户端
  RC write_chunk_result(SqlResult *sql_result);

protected:
  vector<char> send_message_delimiter_;  ///< 发送消息分隔符，用于标识消息的结束
  vector<char> debug_message_prefix_;    ///< 调试信息前缀，用于标记调试信息的开始
};