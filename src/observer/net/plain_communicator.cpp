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
 
#include "net/plain_communicator.h"  // 引入PlainCommunicator类的定义
#include "common/io/io.h"            // 引入IO操作相关函数
#include "common/log/log.h"           // 引入日志记录功能
#include "event/session_event.h"      // 引入会话事件处理
#include "net/buffered_writer.h"       // 引入缓冲写入器
#include "session/session.h"          // 引入会话管理
#include "sql/expr/tuple.h"           // 引入SQL元组表达式

// PlainCommunicator类的构造函数
PlainCommunicator::PlainCommunicator()
{
  send_message_delimiter_.assign(1, '\0');  // 设置消息结束的分隔符为 '\0'
  debug_message_prefix_.resize(2);         // 分配调试信息前缀的空间
  debug_message_prefix_[0] = '#';          // 设置调试信息前缀的第一个字符为 '#'
  debug_message_prefix_[1] = ' ';          // 设置调试信息前缀的第二个字符为 ' '
}

// 从网络读取事件，直到接收到完整的消息
RC PlainCommunicator::read_event(SessionEvent *&event)
{
  RC rc = RC::SUCCESS;  // 初始化返回码为成功

  event = nullptr;  // 初始化事件指针为nullptr

  int data_len = 0;    // 已读取的数据长度
  int read_len = 0;    // 单次读取的长度

  const int max_packet_size = 8192;  // 最大数据包大小
  vector<char> buf(max_packet_size);  // 数据缓冲区

  // 循环读取数据直到遇到 '\0' 字符，表示一条消息的结束
  while (true) {
    read_len = ::read(fd_, buf.data() + data_len, max_packet_size - data_len);
    if (read_len < 0) {  // 如果读取失败
      if (errno == EAGAIN) {  // 如果是EAGAIN错误，继续读取
        continue;
      }
      break;  // 其他错误，跳出循环
    }
    if (read_len == 0) {  // 如果读取到0字节，表示对端关闭连接
      break;
    }

    if (read_len + data_len > max_packet_size) {  // 如果数据超过最大包大小
      data_len += read_len;
      break;  // 跳出循环
    }

    bool msg_end = false;  // 标记是否读取到消息结束
    for (int i = 0; i < read_len; i++) {
      if (buf[data_len + i] == '\0') {  // 检查是否读取到 '\0'
        data_len += i + 1;  // 更新已读取数据的长度
        msg_end = true;  // 标记消息结束
        break;  // 跳出循环
      }
    }

    if (msg_end) {  // 如果读取到消息结束符
      break;  // 跳出循环
    }

    data_len += read_len;  // 更新已读取数据的长度
  }

  if (data_len > max_packet_size) {  // 如果数据超过最大包大小
    LOG_WARN("The length of sql exceeds the limitation %d", max_packet_size);  // 记录日志
    return RC::IOERR_TOO_LONG;  // 返回错误码
  }
  if (read_len == 0) {  // 如果读取到0字节
    LOG_INFO("The peer has been closed %s", addr());  // 记录日志
    return RC::IOERR_CLOSE;  // 返回错误码
  } else if (read_len < 0) {  // 如果读取失败
    LOG_ERROR("Failed to read socket of %s, %s", addr(), strerror(errno));  // 记录日志
    return RC::IOERR_READ;  // 返回错误码
  }

  LOG_INFO("receive command(size=%d): %s", data_len, buf.data());  // 记录接收到的命令
  event = new SessionEvent(this);  // 创建一个新的SessionEvent对象
  event->set_query(string(buf.data(), data_len));  // 设置SessionEvent的查询字符串
  return rc;  // 返回成功
}

// 将执行状态写入到客户端
RC PlainCommunicator::write_state(SessionEvent *event, bool &need_disconnect)
{
  SqlResult *sql_result = event->sql_result();  // 获取SQL执行结果
  const int buf_size = 2048;  // 缓冲区大小
  char *buf = new char[buf_size];  // 分配缓冲区

  const string &state_string = sql_result->state_string();  // 获取状态字符串
  if (state_string.empty()) {  // 如果状态字符串为空
    const char *result = RC::SUCCESS == sql_result->return_code() ? "SUCCESS" : "FAILURE";
    snprintf(buf, buf_size, "%s\n", result);  // 格式化状态信息
  } else {
    snprintf(buf, buf_size, "%s > %s\n", strrc(sql_result->return_code()), state_string.c_str());  // 格式化状态信息
  }

  RC rc = writer_->writen(buf, strlen(buf));  // 写入状态信息
  if (OB_FAIL(rc)) {  // 如果写入失败
    LOG_WARN("failed to send data to client. err=%s", strerror(errno));  // 记录日志
    need_disconnect = true;  // 标记需要断开连接
    delete[] buf;  // 释放缓冲区
    return RC::IOERR_WRITE;  // 返回写入错误
  }

  need_disconnect = false;  // 标记不需要断开连接
  delete[] buf;  // 释放缓冲区

  return RC::SUCCESS;  // 返回成功
}

// 将调试信息写入到客户端
RC PlainCommunicator::write_debug(SessionEvent *request, bool &need_disconnect)
{
  if (!session_->sql_debug_on()) {  // 如果调试模式未开启
    return RC::SUCCESS;  // 返回成功
  }

  SqlDebug &sql_debug = request->sql_debug();  // 获取SQL调试信息
  const list<string> &debug_infos = sql_debug.get_debug_infos();  // 获取调试信息列表
  for (auto &debug_info : debug_infos) {  // 遍历调试信息列表
    RC rc = writer_->writen(debug_message_prefix_.data(), debug_message_prefix_.size());  // 写入调试信息前缀
    if (OB_FAIL(rc)) {  // 如果写入失败
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));  // 记录日志
      need_disconnect = true;  // 标记需要断开连接
      return RC::IOERR_WRITE;  // 返回写入错误
    }

    rc = writer_->writen(debug_info.data(), debug_info.size());  // 写入调试信息
    if (OB_FAIL(rc)) {  // 如果写入失败
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));  // 记录日志
      need_disconnect = true;  // 标记需要断开连接
      return RC::IOERR_WRITE;  // 返回写入错误
    }

    char newline = '\n';  // 新行字符
    rc = writer_->writen(&newline, 1);  // 写入新行字符
    if (OB_FAIL(rc)) {  // 如果写入失败
      LOG_WARN("failed to send new line to client. err=%s", strerror(errno));  // 记录日志
      need_disconnect = true;  // 标记需要断开连接
      return RC::IOERR_WRITE;  // 返回写入错误
    }
  }

  need_disconnect = false;  // 标记不需要断开连接
  return RC::SUCCESS;  // 返回成功
}
 // 写入结果到客户端，包括状态和调试信息
RC PlainCommunicator::write_result(SessionEvent *event, bool &need_disconnect) {
  // 首先，尝试写入结果，如果结果不为空，则需要发送状态信息和调试信息
  RC rc = write_result_internal(event, need_disconnect);
  // 如果不需要断开连接，尝试发送调试信息
  if (!need_disconnect) {
    RC rc1 = write_debug(event, need_disconnect);
    // 如果发送调试信息失败，则记录警告日志
    if (OB_FAIL(rc1)) {
      LOG_WARN("failed to send debug info to client. rc=%s, err=%s", strrc(rc1), strerror(errno));
    }
  }
  // 如果不需要断开连接，发送消息分隔符
  if (!need_disconnect) {
    rc = writer_->writen(send_message_delimiter_.data(), send_message_delimiter_.size());
    // 如果发送消息分隔符失败，则记录错误日志，并断开连接
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to send data back to client. ret=%s, error=%s", strrc(rc), strerror(errno));
      need_disconnect = true;
      return rc;
    }
  }
  // 刷新缓冲区，确保所有数据都被发送
  writer_->flush();  // TODO handle error
  // 返回操作结果
  return rc;
}

// 内部写入结果到客户端，处理结果状态和结果集
RC PlainCommunicator::write_result_internal(SessionEvent *event, bool &need_disconnect) {
  RC rc = RC::SUCCESS;

  need_disconnect = true;

  SqlResult *sql_result = event->sql_result();
  // 如果返回码不是成功，或者结果集中没有操作符（即没有结果要返回），则只发送状态信息
  if (RC::SUCCESS != sql_result->return_code() || !sql_result->has_operator()) {
    return write_state(event, need_disconnect);
  }
  // 打开结果集，准备发送数据
  rc = sql_result->open();
  if (OB_FAIL(rc)) {
    sql_result->close();
    sql_result->set_return_code(rc);
    return write_state(event, need_disconnect);
  }
  // 获取结果集的模式和列数
  const TupleSchema &schema = sql_result->tuple_schema();
  const int cell_num = schema.cell_num();

  // 发送结果集的列名
  for (int i = 0; i < cell_num; i++) {
    const TupleCellSpec &spec = schema.cell_at(i);
    const char *alias = spec.alias();
    if (nullptr != alias || alias[0] != 0) {
      if (0 != i) {
        const char *delim = " | ";
        rc = writer_->writen(delim, strlen(delim));
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to send data to client. err=%s", strerror(errno));
          return rc;
        }
      }
      int len = strlen(alias);
      rc = writer_->writen(alias, len);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to send data to client. err=%s", strerror(errno));
        sql_result->close();
        return rc;
      }
    }
  }

  // 如果结果集有列，则发送列名后的换行符
  if (cell_num > 0) {
    char newline = '\n';
    rc = writer_->writen(&newline, 1);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));
      sql_result->close();
      return rc;
    }
  }

  // 根据执行模式发送结果集，可能是逐行发送（tuple）或分块发送（chunk）
  rc = RC::SUCCESS;
  if (event->session()->get_execution_mode() == ExecutionMode::CHUNK_ITERATOR
      && event->session()->used_chunk_mode()) {
    rc = write_chunk_result(sql_result);
  } else {
    rc = write_tuple_result(sql_result);
  }

  if (OB_FAIL(rc)) {
    return rc;
  }

  // 如果结果集为空（即没有列），则发送状态信息
  if (cell_num == 0) {
    RC rc_close = sql_result->close();
    if (rc == RC::SUCCESS) {
      rc = rc_close;
    }
    sql_result->set_return_code(rc);
    return write_state(event, need_disconnect);
  } else {
    need_disconnect = false;
  }

  // 关闭结果集，并返回操作结果
  RC rc_close = sql_result->close();
  if (OB_SUCC(rc)) {
    rc = rc_close;
  }
  return rc;
}

// 写入元组结果到客户端
RC PlainCommunicator::write_tuple_result(SqlResult *sql_result) {
  RC rc = RC::SUCCESS;
  Tuple *tuple = nullptr;
  // 循环发送每一行数据，直到没有更多的数据
  while (RC::SUCCESS == (rc = sql_result->next_tuple(tuple))) {
    assert(tuple != nullptr);

    int cell_num = tuple->cell_num();
    for (int i = 0; i < cell_num; i++) {
      if (i != 0) {
        const char *delim = " | ";
        rc = writer_->writen(delim, strlen(delim));
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to send data to client. err=%s", strerror(errno));
          sql_result->close();
          return rc;
        }
      }
      Value value;
      rc = tuple->cell_at(i, value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get tuple cell value. rc=%s", strrc(rc));
        sql_result->close();
        return rc;
      }
      string cell_str = value.to_string();
      rc = writer_->writen(cell_str.data(), cell_str.size());
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to send data to client. err=%s", strerror(errno));
        sql_result->close();
        return rc;
      }
    }

    char newline = '\n';
    rc = writer_->writen(&newline, 1);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));
      sql_result->close();
      return rc;
    }
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;  // 如果到达记录的末尾，将返回码设置为成功
  }
  return rc;
}

// 写入块结果到客户端
RC PlainCommunicator::write_chunk_result(SqlResult *sql_result) {
  RC rc = RC::SUCCESS;
  Chunk chunk;
  // 循环发送数据块，直到没有更多的数据
  while (RC::SUCCESS == (rc = sql_result->next_chunk(chunk))) {
    int col_num = chunk.column_num();
    for (int row_idx = 0; row_idx < chunk.rows(); row_idx++) {
      for (int col_idx = 0; col_idx < col_num; col_idx++) {
        if (col_idx != 0) {
          const char *delim = " | ";
          rc = writer_->writen(delim, strlen(delim));
          if (OB_FAIL(rc)) {
            LOG_WARN("failed to send data to client. err=%s", strerror(errno));
            sql_result->close();
            return rc;
          }
        }
        Value value = chunk.get_value(col_idx, row_idx);
        string cell_str = value.to_string();
        rc = writer_->writen(cell_str.data(), cell_str.size());
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to send data to client. err=%s", strerror(errno));
          sql_result->close();
          return rc;
        }
      }
      char newline = '\n';
      rc = writer_->writen(&newline, 1);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to send data to client. err=%s", strerror(errno));
        sql_result->close();
        return rc;
      }
    }
    chunk.reset();
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;  // 如果到达记录的末尾，将返回码设置为成功
  }
  return rc;
}