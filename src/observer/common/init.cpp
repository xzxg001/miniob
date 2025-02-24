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

#include "common/init.h"  // 引入初始化头文件

#include "common/conf/ini.h"                  // 引入INI配置处理
#include "common/lang/string.h"               // 引入字符串工具函数
#include "common/lang/iostream.h"             // 引入输入输出流工具
#include "common/log/log.h"                   // 引入日志功能
#include "common/os/path.h"                   // 引入路径工具
#include "common/os/pidfile.h"                // 引入PID文件处理
#include "common/os/process.h"                // 引入进程管理工具
#include "common/os/signal.h"                 // 引入信号处理工具
#include "global_context.h"                   // 引入全局上下文定义
#include "session/session.h"                  // 引入会话管理
#include "session/session_stage.h"            // 引入会话阶段管理
#include "sql/plan_cache/plan_cache_stage.h"  // 引入执行计划缓存管理
#include "storage/buffer/disk_buffer_pool.h"  // 引入磁盘缓冲池管理
#include "storage/default/default_handler.h"  // 引入默认存储处理器
#include "storage/trx/trx.h"                  // 引入事务处理

using namespace common;  // 使用common命名空间

/**
 * @brief 获取初始化状态的指针
 *
 * @return bool* 指向初始化状态的指针
 */
bool *&_get_init()
{
  static bool  util_init   = false;       // 静态变量，用于保存初始化状态
  static bool *util_init_p = &util_init;  // 指向初始化状态的指针
  return util_init_p;                     // 返回指向初始化状态的指针
}

/**
 * @brief 获取当前的初始化状态
 *
 * @return bool 当前的初始化状态，true表示已初始化，false表示未初始化
 */
bool get_init() { return *_get_init(); }

/**
 * @brief 设置初始化状态
 *
 * @param value 初始化状态，true表示已初始化，false表示未初始化
 */
void set_init(bool value) { *_get_init() = value; }

/**
 * @brief 信号处理函数，用于处理接收到的信号
 *
 * @param sig 接收到的信号编号
 */
void sig_handler(int sig)
{
  LOG_INFO("Receive one signal of %d.", sig);  // 记录接收到的信号
}

/**
 * @brief 初始化日志
 *
 * @param process_cfg 进程参数配置
 * @param properties 配置文件属性
 * @return int 返回0表示成功，其他值表示错误
 */
int init_log(ProcessParam *process_cfg, Ini &properties)
{
  const string &proc_name = process_cfg->get_process_name();  // 获取进程名称
  try {
    if (g_log) {  // 检查日志是否已经初始化
      return 0;   // 已经初始化，无需操作
    }

    // 定义日志上下文获取器
    auto log_context_getter = []() { return reinterpret_cast<intptr_t>(Session::current_session()); };

    const string        log_section_name = "LOG";                             // 定义日志部分名称
    map<string, string> log_section      = properties.get(log_section_name);  // 获取日志部分

    string log_file_name;  // 用于保存日志文件名称的变量

    // 从属性中获取日志文件名称
    string key = "LOG_FILE_NAME";
    auto   it  = log_section.find(key);
    if (it == log_section.end()) {
      log_file_name = proc_name + ".log";  // 默认日志文件名称
      cout << "Not set log file name, use default " << log_file_name << endl;
    } else {
      log_file_name = it->second;  // 自定义日志文件名称
    }

    log_file_name = getAboslutPath(log_file_name.c_str());  // 获取日志文件的绝对路径

    LOG_LEVEL log_level = LOG_LEVEL_INFO;    // 默认日志级别
    key                 = "LOG_FILE_LEVEL";  // 日志级别的键
    it                  = log_section.find(key);
    if (it != log_section.end()) {
      int log = (int)log_level;
      str_to_val(it->second, log);  // 将字符串转换为日志级别
      log_level = (LOG_LEVEL)log;   // 设置日志级别
    }

    LOG_LEVEL console_level = LOG_LEVEL_INFO;       // 默认控制台日志级别
    key                     = "LOG_CONSOLE_LEVEL";  // 控制台日志级别的键
    it                      = log_section.find(key);
    if (it != log_section.end()) {
      int log = (int)console_level;
      str_to_val(it->second, log);     // 将字符串转换为控制台日志级别
      console_level = (LOG_LEVEL)log;  // 设置控制台日志级别
    }

    LoggerFactory::init_default(log_file_name, log_level, console_level);  // 初始化日志工厂
    g_log->set_context_getter(log_context_getter);                         // 设置日志上下文获取器

    key = "DefaultLogModules";  // 默认日志模块的键
    it  = log_section.find(key);
    if (it != log_section.end()) {
      g_log->set_default_module(it->second);  // 设置默认日志模块
    }

    if (process_cfg->is_demon()) {
      sys_log_redirect(log_file_name.c_str(), log_file_name.c_str());  // 重定向系统日志
    }

    return 0;  // 成功初始化日志
  } catch (exception &e) {
    cerr << "Failed to init log for " << proc_name << SYS_OUTPUT_FILE_POS << SYS_OUTPUT_ERROR
         << endl;  // 日志初始化失败的错误信息
    return errno;  // 返回错误码
  }

  return 0;  // 成功
}

/**
 * @brief 清理日志
 */
void cleanup_log()
{
  if (g_log) {
    delete g_log;     // 删除日志对象
    g_log = nullptr;  // 将指针置空
  }
}

/**
 * @brief 准备SEDA（流式事件驱动架构）初始化
 *
 * @return int 返回0表示成功，其他值表示错误
 */
int prepare_init_seda()
{
  return 0;  // 暂无实现，返回0
}

/**
 * @brief 初始化全局对象
 *
 * @param process_param 进程参数配置
 * @param properties 配置文件属性
 * @return int 返回0表示成功，其他值表示错误
 */
int init_global_objects(ProcessParam *process_param, Ini &properties)
{
  GCTX.handler_ = new DefaultHandler();  // 创建默认处理器

  int ret = 0;  // 返回值

  RC rc = GCTX.handler_->init("miniob",  // 初始化处理器
      process_param->trx_kit_name().c_str(),
      process_param->durability_mode().c_str());
  if (OB_FAIL(rc)) {
    LOG_ERROR("failed to init handler. rc=%s", strrc(rc));  // 处理器初始化失败的错误信息
    return -1;                                             // 返回错误码
  }
  return ret;  // 返回成功
}

/**
 * @brief 反初始化全局对象
 *
 * @return int 返回0表示成功
 */
int uninit_global_objects()
{
  delete GCTX.handler_;     // 删除处理器对象
  GCTX.handler_ = nullptr;  // 将指针置空

  return 0;  // 返回成功
}

/**
 * @brief 初始化函数
 *
 * @param process_param 进程参数配置
 * @return int 返回0表示成功，其他值表示错误
 */
int init(ProcessParam *process_param)
{
  if (get_init()) {  // 如果已初始化
    return 0;        // 返回成功
  }

  set_init(true);  // 设置初始化状态为true

  // 如果请求以守护进程身份运行
  int rc = STATUS_SUCCESS;
  if (process_param->is_demon()) {
    rc = daemonize_service(process_param->get_std_out().c_str(), process_param->get_std_err().c_str());  // 守护进程化
    if (rc != 0) {
      cerr << "Shutdown due to failed to daemon current process!" << endl;  // 守护进程化失败的错误信息
      return rc;                                                            // 返回错误码
    }
  }

  writePidFile(process_param->get_process_name().c_str());  // 写入PID文件

  // 在进入多线程模式之前初始化全局变量
  // 以避免竞争条件

  // 读取配置文件
  rc = get_properties()->load(process_param->get_conf());
  if (rc) {
    cerr << "Failed to load configuration files" << endl;  // 加载配置文件失败的错误信息
    return rc;                                             // 返回错误码
  }

  // 初始化跟踪器
  rc = init_log(process_param, *get_properties());
  if (rc) {
    cerr << "Failed to init Log" << endl;  // 日志初始化失败的错误信息
    return rc;                             // 返回错误码
  }

  string conf_data;
  get_properties()->to_string(conf_data);                   // 将配置转换为字符串
  LOG_INFO("Output configuration \n%s", conf_data.c_str());  // 输出配置

  rc = init_global_objects(process_param, *get_properties());
  if (rc != 0) {
    LOG_ERROR("failed to init global objects");  // 全局对象初始化失败的错误信息
    return rc;                                   // 返回错误码
  }

  // 在创建子线程之前阻塞中断信号
  // setSignalHandler(sig_handler);
  // sigset_t newSigset, oset;
  // blockDefaultSignals(&newSigset, &oset);
  // 等待中断信号
  // startWaitForSignals(&newSigset);

  LOG_INFO("Successfully init utility");  // 成功初始化的日志信息

  return STATUS_SUCCESS;  // 返回成功
}

/**
 * @brief 清理函数
 */
void cleanup_util()
{
  uninit_global_objects();  // 反初始化全局对象

  if (nullptr != get_properties()) {
    delete get_properties();     // 删除配置对象
    get_properties() = nullptr;  // 将指针置空
  }

  LOG_INFO("Shutdown Cleanly!");  // 优雅关机的日志信息

  // 清理跟踪器
  cleanup_log();

  set_init(false);  // 设置初始化状态为false
}

/**
 * @brief 清理函数，调用清理工具
 */
void cleanup() { cleanup_util(); }  // 调用清理函数
