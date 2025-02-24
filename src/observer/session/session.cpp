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
// Created by Wangyunlai on 2021/5/12.
//

#include "session/session.h"
#include "common/global_context.h"
#include "storage/db/db.h"
#include "storage/default/default_handler.h"
#include "storage/trx/trx.h"

/**
 * @brief 获取默认的Session实例
 * 
 * 该函数实现了Singleton模式，确保在整个程序中只有一个Session实例被创建和使用
 * 它使用了局部静态变量来保证实例在第一次调用时被创建，并在程序结束时自动销毁
 * 这种实现方式是线程安全的，自C++11标准开始
 * 
 * @return Session& 返回默认Session实例的引用，可以用于访问Session类的成员函数和属性
 */
Session &Session::default_session()
{
  // 静态变量，确保在整个程序生命周期中只被创建和销毁一次
  static Session session;
  // 返回静态变量的引用，供外部使用
  return session;
}

// 复制构造函数，用于创建与现有会话相同的会话
Session::Session(const Session &other) : db_(other.db_) {}

// Session类的析构函数
Session::~Session()
{
  // 检查当前Session对象是否关联了一个事务trx_
  if (nullptr != trx_) {
    // 如果存在关联事务，调用数据库的事务管理套件 trx_kit 的 destroy_trx 方法来销毁该事务
    db_->trx_kit().destroy_trx(trx_);
    // 销毁事务后，将trx_指针设置为nullptr，表示此Session不再关联任何事务
    trx_ = nullptr;
  }
}

// 获取当前数据库名称
// 
// 当前函数通过检查session中关联的数据库指针是否为空，来决定返回的数据库名称。
// 如果db_指针不为空，则调用其name()方法获取数据库名称；否则，返回空字符串。
// 这个函数允许外部获取当前session关联的数据库的名称，以便在需要时进行显示或进一步处理。
const char *Session::get_current_db_name() const
{
  // 检查数据库指针是否为空
  if (db_ != nullptr)
    // 如果不为空，则返回数据库的名称
    return db_->name();
  else
    // 如果为空，则返回空字符串
    return "";
}

/**
 * 获取当前会话关联的数据库实例指针
 * 
 * 该函数用于返回当前会话正在使用的数据库（Db）实例的指针
 * 它允许调用者访问或操作会话特定的数据库配置、状态或其他相关信息
 * 
 * @return Db* 返回一个指向数据库实例的指针如果会话未关联数据库，则可能返回nullptr
 */
Db *Session::get_current_db() const { return db_; }

/**
 * 设置当前数据库
 * 
 * 此函数用于切换Session实例的当前数据库它通过数据库名称查找对应的数据库对象，
 * 如果找到，则将该数据库对象设置为当前数据库；如果未找到，则记录警告日志
 * 
 * @param dbname 数据库名称，用于识别和查找数据库
 */
void Session::set_current_db(const string &dbname)
{
  // 获取默认处理器实例，用于数据库操作
  DefaultHandler &handler = *GCTX.handler_;
  // 通过数据库名称查找数据库对象
  Db             *db      = handler.find_db(dbname.c_str());
  // 如果未找到数据库，记录警告日志并退出函数
  if (db == nullptr) {
    LOG_WARN("no such database: %s", dbname.c_str());
    return;
  }

  // 记录切换数据库的跟踪日志
  LOG_TRACE("change db to %s", dbname.c_str());
  // 将找到的数据库对象设置为当前数据库
  db_ = db;
}

/**
 * 设置事务的多操作模式
 * 
 * @param multi_operation_mode 指定事务是否以多操作模式运行
 * 
 * 此函数用于配置事务的多操作模式，该模式决定了事务如何处理多个操作
 * 在多操作模式下，事务可以累积多个操作，并在它们之间保持一致性
 * 如果不需要多操作模式，事务将每个操作视为独立的单位进行处理
 */
void Session::set_trx_multi_operation_mode(bool multi_operation_mode)
{
  trx_multi_operation_mode_ = multi_operation_mode;
}

// 返回当前会话是否处于多操作事务模式
/**
 * 此函数用于判断当前会话对象是否处于可以执行多个操作的事务模式。
 * 事务模式是指在数据库操作中，多个操作被当作一个单一的、不可分割的工作单元来执行。
 * 如果会话处于多操作事务模式，返回值将为true，否则为false。
 * 
 * @return bool 表示当前会话是否处于多操作事务模式的布尔值。
 */
bool Session::is_trx_multi_operation_mode() const { return trx_multi_operation_mode_; }

/**
 * 获取当前会话中的事务对象
 * 
 * 此函数负责返回当前会话中正在进行的事务对象如果当前没有事务，
 * 则会创建一个新的事务对象并与当前会话绑定这一设计假设了一个会话
 * 只会与一个数据库相关联，尽管这在现实中可能并不总是合理但为了简化处理过程，
 * 并且考虑到在测试环境中我们不需要处理与多个数据库的关联，这种设计是可以接受的
 * 
 * @return Trx* 返回当前会话中的事务对象指针
 */
Trx *Session::current_trx()
{
  /*
  当前把事务与数据库绑定到了一起。这样虽然不合理，但是处理起来也简单。
  我们在测试过程中，也不需要多个数据库之间做关联。
  */
  if (trx_ == nullptr) {
    trx_ = db_->trx_kit().create_trx(db_->log_handler());
  }
  return trx_;
}

thread_local Session *thread_session = nullptr;

// 设置当前线程的会话对象
// 参数 session: 指向要设置为当前线程会话的Session对象
void Session::set_current_session(Session *session) { thread_session = session; }

/**
 * @brief 获取当前线程的会话
 * 
 * 该函数返回一个指向当前线程正在使用的会话对象的指针。
 * 它用于多线程环境中管理每个线程独立的会话状态。
 * 
 * @return Session* 当前线程的会话对象指针
 */
Session *Session::current_session() { return thread_session; }

// 设置当前会话的请求事件
// 参数 request: 指向请求事件的指针，通过此参数将请求事件与会话关联起来
void Session::set_current_request(SessionEvent *request) { current_request_ = request; }

/**
 * @brief 获取当前的会话事件请求
 * 
 * 该函数用于返回当前正在处理的会话事件请求。通过此函数，可以访问会话中的当前请求，
 * 以便进行进一步的处理或获取请求相关的信息。
 * 
 * @return SessionEvent* 返回指向当前会话事件请求的指针。
 */
SessionEvent *Session::current_request() const { return current_request_; }
