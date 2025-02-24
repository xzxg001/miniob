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

// 包含必要的头文件
#include <setjmp.h> // 用于信号处理
#include <signal.h> // 用于信号处理

#include "net/cli_communicator.h" // CLI通信器的头文件
#include "common/lang/string.h"    // 字符串处理
#include "common/log/log.h"        // 日志记录
#include "common/os/signal.h"      // 操作系统信号处理
#include "event/session_event.h"    // 会话事件
#include "net/buffered_writer.h"    // 缓冲写入器
#include "session/session.h"        // 会话管理

#ifdef USE_READLINE
#include "readline/history.h"       // 历史记录
#include "readline/readline.h"      // readline库
#endif

#define MAX_MEM_BUFFER_SIZE 8192    // 最大内存缓冲区大小
#define PORT_DEFAULT 6789            // 默认端口号

using namespace common; // 使用common命名空间

#ifdef USE_READLINE
// 历史文件路径
const string HISTORY_FILE = string(getenv("HOME")) + "/.miniob.history";
time_t last_history_write_time = 0; // 上次写入历史记录的时间
sigjmp_buf ctrlc_buf; // 用于信号处理的跳转缓冲区
bool ctrlc_flag = false; // 控制Ctrl+C标志

// 信号处理函数
void handle_signals(int signo) {
  if (signo == SIGINT) { // 捕获SIGINT信号
    ctrlc_flag = true; // 设置标志
    siglongjmp(ctrlc_buf, 1); // 跳转到setjmp的位置
  }
}

// 自定义读取行的函数
char *my_readline(const char *prompt) {
  static sighandler_t setup_signal_handler = signal(SIGINT, handle_signals); // 设置信号处理
  (void)setup_signal_handler; // 防止未使用警告

  int size = history_length; // 获取历史记录长度
  if (size == 0) { // 如果没有历史记录
    read_history(HISTORY_FILE.c_str()); // 读取历史记录

    FILE *fp = fopen(HISTORY_FILE.c_str(), "a"); // 打开历史文件
    if (fp != nullptr) {
      fclose(fp); // 关闭文件
    }
  }

// 使用setjmp设置跳转点
  while (sigsetjmp(ctrlc_buf, 1) != 0);

  if (ctrlc_flag) { // 如果捕获到Ctrl+C
    char *line = (char *)malloc(strlen("exit") + 1); // 分配内存
    strcpy(line, "exit"); // 设置返回值为"exit"
    printf("\n");
    return line; // 返回"exit"
  }

  char *line = readline(prompt); // 使用readline读取输入
  if (line != nullptr && line[0] != 0) { // 如果输入有效
    add_history(line); // 添加到历史记录
    if (time(NULL) - last_history_write_time > 5) { // 每5秒写入一次历史记录
      write_history(HISTORY_FILE.c_str());
    }
  }
  return line; // 返回输入
}
#else   // USE_READLINE
// 如果没有使用readline，使用fgets读取输入
char *my_readline(const char *prompt) {
  char *buffer = (char *)malloc(MAX_MEM_BUFFER_SIZE); // 分配缓冲区
  if (nullptr == buffer) {
    LOG_WARN("failed to alloc line buffer"); // 分配失败
    return nullptr;
  }
  fprintf(stdout, "%s", prompt); // 打印提示符
  char *s = fgets(buffer, MAX_MEM_BUFFER_SIZE, stdin); // 读取输入
  if (nullptr == s) { // 如果读取失败
    if (ferror(stdin) || feof(stdin)) {
      LOG_WARN("failed to read line: %s", strerror(errno)); // 记录错误
    }
    // EINTR(4):Interrupted system call
    if (errno == EINTR) { // 如果被中断
      strncpy(buffer, "interrupted", MAX_MEM_BUFFER_SIZE); // 设置返回值为"interrupted"
      fprintf(stdout, "\n");
      return buffer; // 返回缓冲区
    }
    free(buffer); // 释放缓冲区
    return nullptr;
  }
  return buffer; // 返回缓冲区
}
#endif  // USE_READLINE


/* this function config a exit-cmd list, strncasecmp func truncate the command from terminal according to the number,
   'strncasecmp("exit", cmd, 4)' means that obclient read command string from terminal, truncate it to 4 chars from
   the beginning, then compare the result with 'exit', if they match, exit the obclient.
*/
// 检查输入命令是否为退出命令
bool is_exit_command(const char *cmd) {
  return 0 == strncasecmp("exit", cmd, 4) 
      || 0 == strncasecmp("bye", cmd, 3) 
      || 0 == strncasecmp("\\q", cmd, 2)
      || 0 == strncasecmp("interrupted", cmd, 11);
}

// 读取用户输入的命令
char *read_command() {
  const char *prompt_str = "miniob > "; // 提示符
  char *input_command = my_readline(prompt_str); // 读取命令
  return input_command; // 返回命令
}


// 初始化CLI通信器
RC CliCommunicator::init(int fd, unique_ptr<Session> session, const string &addr) {
  RC rc = PlainCommunicator::init(fd, std::move(session), addr); // 调用父类初始化
  if (OB_FAIL(rc)) {
    LOG_WARN("fail to init communicator", strrc(rc)); // 初始化失败
    return rc;
  }

  if (fd == STDIN_FILENO) { // 仅支持标准输入
    write_fd_ = STDOUT_FILENO; // 设置写入文件描述符
    delete writer_; // 删除旧的写入器
    writer_ = new BufferedWriter(write_fd_); // 创建新的缓冲写入器

    const char delimiter = '\n'; // 消息分隔符
    send_message_delimiter_.assign(1, delimiter); // 设置分隔符

    fd_ = -1;  // 防止被父类析构函数关闭
  } else {
    rc = RC::INVALID_ARGUMENT; // 非法参数
    LOG_WARN("only stdin supported"); // 记录警告
  }
  return rc; // 返回结果
}

// 读取事件
RC CliCommunicator::read_event(SessionEvent *&event) {
  event = nullptr; // 初始化事件
  char *command = read_command(); // 读取命令
  if (nullptr == command) {
    return RC::SUCCESS; // 读取失败
  }

  if (is_blank(command)) { // 如果命令为空
    free(command); // 释放命令
    return RC::SUCCESS; // 返回成功
  }

  if (is_exit_command(command)) { // 如果是退出命令
    free(command); // 释放命令
    exit_ = true; // 设置退出标志
    return RC::SUCCESS; // 返回成功
  }

  event = new SessionEvent(this); // 创建新的会话事件
  event->set_query(string(command)); // 设置查询内容
  free(command); // 释放命令
  return RC::SUCCESS; // 返回成功
}

// 写入结果
RC CliCommunicator::write_result(SessionEvent *event, bool &need_disconnect) {
  RC rc = PlainCommunicator::write_result(event, need_disconnect); // 调用父类方法

  need_disconnect = false; // 设置不需要断开连接
  return rc; // 返回结果
}