/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/4/13.
// 

#include "storage/default/default_handler.h"

#include <string>
#include <filesystem>

#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/os/path.h"
#include "session/session.h"
#include "storage/common/condition_filter.h"
#include "storage/index/bplus_tree.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

DefaultHandler::DefaultHandler() {} // 默认处理器构造函数

DefaultHandler::~DefaultHandler() noexcept { destroy(); } // 默认处理器析构函数，调用销毁方法

RC DefaultHandler::init(const char *base_dir, const char *trx_kit_name, const char *log_handler_name)
{
  // 检查目录是否存在，如果不存在则创建
  filesystem::path db_dir(base_dir);
  db_dir /= "db"; // 数据库目录为 base_dir/db
  error_code ec;
  if (!filesystem::is_directory(db_dir) && !filesystem::create_directories(db_dir, ec)) {
    LOG_ERROR("Cannot access base dir: %s. msg=%d:%s", db_dir.c_str(), errno, strerror(errno));
    return RC::INTERNAL; // 返回内部错误
  }

  // 初始化成员变量
  base_dir_ = base_dir;
  db_dir_   = db_dir;
  trx_kit_name_ = trx_kit_name;
  log_handler_name_ = log_handler_name;

  const char *sys_db = "sys"; // 系统数据库名

  // 创建系统数据库
  RC ret = create_db(sys_db);
  if (ret != RC::SUCCESS && ret != RC::SCHEMA_DB_EXIST) {
    LOG_ERROR("Failed to create system db");
    return ret; // 返回创建系统数据库失败的状态
  }

  // 打开系统数据库
  ret = open_db(sys_db);
  if (ret != RC::SUCCESS) {
    LOG_ERROR("Failed to open system db. rc=%s", strrc(ret));
    return ret; // 返回打开系统数据库失败的状态
  }

  Session &default_session = Session::default_session(); // 获取默认会话
  default_session.set_current_db(sys_db); // 设置当前数据库为系统数据库

  LOG_INFO("Default handler init with %s success", base_dir);
  return RC::SUCCESS; // 返回成功状态
}

void DefaultHandler::destroy()
{
  sync(); // 同步所有数据库

  // 删除所有打开的数据库
  for (const auto &iter : opened_dbs_) {
    delete iter.second; // 释放内存
  }
  opened_dbs_.clear(); // 清空打开的数据库映射
}

RC DefaultHandler::create_db(const char *dbname)
{
  if (nullptr == dbname || common::is_blank(dbname)) {
    LOG_WARN("Invalid db name");
    return RC::INVALID_ARGUMENT; // 返回无效参数
  }

  // 如果数据库已存在，返回错误
  filesystem::path dbpath = db_dir_ / dbname;
  if (filesystem::is_directory(dbpath)) {
    LOG_WARN("Db already exists: %s", dbname);
    return RC::SCHEMA_DB_EXIST; // 数据库已存在
  }

  error_code ec;
  if (!filesystem::create_directories(dbpath, ec)) {
    LOG_ERROR("Create db fail: %s. error=%s", dbpath.c_str(), strerror(errno));
    return RC::IOERR_WRITE; // 创建数据库失败
  }
  return RC::SUCCESS; // 返回成功状态
}

RC DefaultHandler::drop_db(const char *dbname) { return RC::INTERNAL; } // 删除数据库，未实现

RC DefaultHandler::open_db(const char *dbname)
{
  if (nullptr == dbname || common::is_blank(dbname)) {
    LOG_WARN("Invalid db name");
    return RC::INVALID_ARGUMENT; // 返回无效参数
  }

  // 检查数据库是否已打开
  if (opened_dbs_.find(dbname) != opened_dbs_.end()) {
    return RC::SUCCESS; // 数据库已打开，直接返回成功
  }

  filesystem::path dbpath = db_dir_ / dbname; // 数据库路径
  if (!filesystem::is_directory(dbpath)) {
    return RC::SCHEMA_DB_NOT_EXIST; // 数据库不存在
  }

  // 打开数据库
  Db *db  = new Db();
  RC  ret = RC::SUCCESS;
  if ((ret = db->init(dbname, dbpath.c_str(), trx_kit_name_.c_str(), log_handler_name_.c_str())) != RC::SUCCESS) {
    LOG_ERROR("Failed to open db: %s. error=%s", dbname, strrc(ret));
    delete db; // 初始化失败，释放内存
  } else {
    opened_dbs_[dbname] = db; // 将数据库添加到已打开数据库映射中
  }
  return ret; // 返回打开数据库的结果
}

RC DefaultHandler::close_db(const char *dbname) { return RC::UNIMPLEMENTED; } // 关闭数据库，未实现

RC DefaultHandler::create_table(const char *dbname, const char *relation_name, span<const AttrInfoSqlNode> attributes)
{
  Db *db = find_db(dbname); // 查找数据库
  if (db == nullptr) {
    return RC::SCHEMA_DB_NOT_OPENED; // 数据库未打开
  }
  return db->create_table(relation_name, attributes); // 创建表
}

RC DefaultHandler::drop_table(const char *dbname, const char *relation_name) { return RC::UNIMPLEMENTED; } // 删除表，未实现

Db *DefaultHandler::find_db(const char *dbname) const
{
  // 查找打开的数据库
  map<string, Db *>::const_iterator iter = opened_dbs_.find(dbname);
  if (iter == opened_dbs_.end()) {
    return nullptr; // 数据库未找到
  }
  return iter->second; // 返回数据库指针
}

Table *DefaultHandler::find_table(const char *dbname, const char *table_name) const
{
  if (dbname == nullptr || table_name == nullptr) {
    LOG_WARN("Invalid argument. dbname=%p, table_name=%p", dbname, table_name);
    return nullptr; // 参数无效
  }
  Db *db = find_db(dbname); // 查找数据库
  if (nullptr == db) {
    return nullptr; // 数据库未找到
  }

  return db->find_table(table_name); // 查找表
}

RC DefaultHandler::sync()
{
  RC rc = RC::SUCCESS;
  // 遍历所有已打开的数据库并同步
  for (const auto &db_pair : opened_dbs_) {
    Db *db = db_pair.second;
    rc     = db->sync(); // 同步数据库
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to sync db. name=%s, rc=%d:%s", db->name(), rc, strrc(rc));
      return rc; // 返回同步失败状态
    }
  }
  return rc; // 返回成功状态
}
