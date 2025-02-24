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
// Created by Wangyunlai on 2024/02/28.
//

#include "storage/trx/mvcc_trx_log.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/table/table.h"
#include "storage/clog/log_module.h"
#include "storage/clog/log_handler.h"
#include "storage/clog/log_entry.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

using namespace common;

/**
 * @brief 生成当前操作的字符串表示
 * 
 * 该函数将操作的索引和类型转换为一个字符串，用于日志记录或调试目的。
 * 它首先将索引转换为字符串，然后根据操作类型追加相应的字符串描述。
 * 
 * @return string 包含操作索引和类型的字符串
 */
string MvccTrxLogOperation::to_string() const
{
  // 将操作的索引转换为字符串并初始化返回值
  string ret = std::to_string(index()) + ":";
  
  // 根据操作类型追加相应的字符串描述
  switch (type_) {
    case Type::INSERT_RECORD: return ret + "INSERT_RECORD";
    case Type::DELETE_RECORD: return ret + "DELETE_RECORD";
    case Type::COMMIT: return ret + "COMMIT";
    case Type::ROLLBACK: return ret + "ROLLBACK";
    // 如果操作类型未知，追加"UNKNOWN"
    default: return ret + "UNKNOWN";
  }
}

const int32_t MvccTrxLogHeader::SIZE = sizeof(MvccTrxLogHeader);

/**
 * 将MVCC事务日志头信息转换为字符串表示
 * 
 * 此函数用于生成MVCC事务日志头信息的字符串表示，便于日志记录和调试
 * 它包含了操作类型和事务ID，其中操作类型通过MvccTrxLogOperation类转换为字符串
 * 
 * 返回: 包含事务日志头信息的字符串
 */
string MvccTrxLogHeader::to_string() const
{
  // 使用stringstream构建包含操作类型和事务ID的字符串
  stringstream ss;
  ss << "operation_type:" << MvccTrxLogOperation(operation_type).to_string() << ", trx_id:" << trx_id;
  // 返回构建好的字符串
  return ss.str();
}

const int32_t MvccTrxRecordLogEntry::SIZE = sizeof(MvccTrxRecordLogEntry);

/**
 * @brief 生成当前MVCC事务记录日志条目的字符串表示
 * 
 * 该函数用于生成一个包含事务记录日志条目详细信息的字符串表示，主要用于日志记录和调试目的
 * 它会包含日志条目的头部信息、表ID和记录ID
 * 
 * @return string 包含事务记录日志条目信息的字符串
 */
string MvccTrxRecordLogEntry::to_string() const
{
  // 创建一个字符串流对象，用于构建最终的字符串表示
  stringstream ss;

  // 将日志条目的头部信息、表ID和记录ID依次添加到字符串流中
  ss << header.to_string() << ", table_id: " << table_id << ", rid: " << rid.to_string();

  // 将字符串流中的内容转换为字符串并返回
  return ss.str();
}

const int32_t MvccTrxCommitLogEntry::SIZE = sizeof(MvccTrxCommitLogEntry);

/**
 * 生成当前MVCC事务提交日志条目的字符串表示
 * 
 * 此函数用于将MVCC事务提交日志条目的相关信息转换为字符串格式，以便于日志记录和调试
 * 它通过使用stringstream来构建一个包含条目头部信息和提交事务ID的字符串
 * 
 * @return 返回一个字符串，包含了条目的头部信息和提交事务ID
 */
string MvccTrxCommitLogEntry::to_string() const
{
  stringstream ss;
  ss << header.to_string() << ", commit_trx_id: " << commit_trx_id;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 构造函数：初始化MvccTrxLogHandler对象
// 参数 log_handler: 一个LogHandler类型的引用，用于MvccTrxLogHandler对象初始化
MvccTrxLogHandler::MvccTrxLogHandler(LogHandler &log_handler) : log_handler_(log_handler) {}

// MvccTrxLogHandler类的析构函数
// 该析构函数负责释放MvccTrxLogHandler对象在生命周期内创建或获取的资源
// 由于该类的具体实现未提供，此处不涉及具体资源释放的说明
MvccTrxLogHandler::~MvccTrxLogHandler() {}

/**
 * 在MVCC事务日志处理器中插入记录
 * 
 * @param trx_id 事务ID，用于标识事务
 * @param table 表指针，表示记录要插入的表
 * @param rid 记录ID，用于标识表中的记录
 * @return 返回执行结果，表示插入记录操作的成功与否
 * 
 * 此函数的主要职责是创建一个表示插入记录操作的日志条目，并将其添加到日志处理器中
 * 它首先验证事务ID的有效性，然后构造一个日志条目，包含操作类型、事务ID、表ID和记录ID
 * 最后，它使用日志处理器的append方法将日志条目添加到日志中
 */
RC MvccTrxLogHandler::insert_record(int32_t trx_id, Table *table, const RID &rid)
{
  // 确保事务ID大于0，否则打印错误信息
  ASSERT(trx_id > 0, "invalid trx_id:%d", trx_id);

  // 创建一个MVCC事务日志条目
  MvccTrxRecordLogEntry log_entry;
  // 设置日志条目的操作类型为插入记录
  log_entry.header.operation_type = MvccTrxLogOperation(MvccTrxLogOperation::Type::INSERT_RECORD).index();
  // 设置日志条目的事务ID
  log_entry.header.trx_id         = trx_id;
  // 设置日志条目的表ID
  log_entry.table_id              = table->table_id();
  // 设置日志条目的记录ID
  log_entry.rid                   = rid;

  // 初始化LSN（日志序列号）为0
  LSN lsn = 0;
  // 调用日志处理器的append方法，将日志条目添加到日志中
  return log_handler_.append(
      lsn, LogModule::Id::TRANSACTION, span<const char>(reinterpret_cast<const char *>(&log_entry), sizeof(log_entry)));
}

/**
 * 删除记录的MVCC事务日志处理函数
 * 
 * 本函数负责生成并追加一条删除记录的MVCC事务日志到日志系统中
 * 它不直接执行删除操作，而是记录删除操作的事实，以便于后续的事务管理和数据恢复
 * 
 * @param trx_id 事务ID，必须大于0
 * @param table 指向要删除记录的表的指针
 * @param rid 要删除记录的ROW ID
 * 
 * @return 返回事务日志追加的结果，表示操作是否成功
 */
RC MvccTrxLogHandler::delete_record(int32_t trx_id, Table *table, const RID &rid)
{
  // 确保事务ID有效，如果无效则抛出断言错误
  ASSERT(trx_id > 0, "invalid trx_id:%d", trx_id);

  // 初始化MVCC事务日志条目
  MvccTrxRecordLogEntry log_entry;
  log_entry.header.operation_type = MvccTrxLogOperation(MvccTrxLogOperation::Type::DELETE_RECORD).index();
  log_entry.header.trx_id         = trx_id;
  log_entry.table_id              = table->table_id();
  log_entry.rid                   = rid;

  // 初始化日志序列号
  LSN lsn = 0;
  // 调用日志处理器的追加方法，将日志条目追加到日志系统中
  return log_handler_.append(
      lsn, LogModule::Id::TRANSACTION, span<const char>(reinterpret_cast<const char *>(&log_entry), sizeof(log_entry)));
}

/**
 * 提交事务并记录提交日志
 * 
 * 该函数负责将事务标记为已提交，并生成相应的事务提交日志
 * 主要执行以下操作：
 * 1. 校验事务ID和提交事务ID的有效性
 * 2. 创建事务提交日志条目，并填充必要的信息
 * 3. 调用日志处理函数将提交日志写入日志文件
 * 4. 等待日志写入完成并返回结果
 * 
 * @param trx_id 事务ID，表示待提交的事务
 * @param commit_trx_id 提交事务ID，表示事务提交时的全局事务序列号
 * @return RC 返回结果代码，表示操作成功或失败
 */
RC MvccTrxLogHandler::commit(int32_t trx_id, int32_t commit_trx_id)
{
  // 校验事务ID和提交事务ID的有效性，确保它们符合预期的顺序
  ASSERT(trx_id > 0 && commit_trx_id > trx_id, "invalid trx_id:%d, commit_trx_id:%d", trx_id, commit_trx_id);

  // 创建事务提交日志条目，并填充必要的信息
  MvccTrxCommitLogEntry log_entry;
  log_entry.header.operation_type = MvccTrxLogOperation(MvccTrxLogOperation::Type::COMMIT).index();
  log_entry.header.trx_id         = trx_id;
  log_entry.commit_trx_id         = commit_trx_id;

  // 初始化日志序列号(LSN)
  LSN lsn = 0;
  // 调用日志处理函数将提交日志写入日志文件
  RC rc = log_handler_.append(
      lsn, LogModule::Id::TRANSACTION, span<const char>(reinterpret_cast<const char *>(&log_entry), sizeof(log_entry)));
  // 如果日志写入失败，直接返回错误代码
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 我们在这里粗暴的等待日志写入到磁盘
  // 有必要的话，可以让上层来决定如何等待
  // 等待日志写入完成并返回结果
  return log_handler_.wait_lsn(lsn);
}

/**
 * 回滚指定事务
 * 
 * 本函数通过生成一个回滚类型的事务日志条目，并将其追加到日志处理器中，来实现事务的回滚
 * 
 * @param trx_id 事务ID，用于标识需要回滚的事务必须大于0
 * @return RC 返回事务回滚的操作结果
 */
RC MvccTrxLogHandler::rollback(int32_t trx_id)
{
  // 确保事务ID有效，如果无效则抛出断言错误
  ASSERT(trx_id > 0, "invalid trx_id:%d", trx_id);

  // 创建一个事务日志条目，用于记录回滚操作
  MvccTrxCommitLogEntry log_entry;
  // 设置日志条目的操作类型为回滚
  log_entry.header.operation_type = MvccTrxLogOperation(MvccTrxLogOperation::Type::ROLLBACK).index();
  // 设置日志条目的事务ID
  log_entry.header.trx_id         = trx_id;

  // 初始化LSN（日志序列号）为0，用于记录日志条目的位置
  LSN lsn = 0;
  // 将日志条目追加到日志处理器中，完成回滚操作的记录
  return log_handler_.append(
      lsn, LogModule::Id::TRANSACTION, span<const char>(reinterpret_cast<const char *>(&log_entry), sizeof(log_entry)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
MvccTrxLogReplayer::MvccTrxLogReplayer(Db &db, MvccTrxKit &trx_kit, LogHandler &log_handler)
  : db_(db), trx_kit_(trx_kit), log_handler_(log_handler)
{}

// 重放事务日志条目
// 该函数负责解析日志条目，并根据条目中的信息重放事务操作
RC MvccTrxLogReplayer::replay(const LogEntry &entry)
{
  RC rc = RC::SUCCESS;

  // 由于当前没有check point，所以所有的事务都重做。

  // 确保日志条目属于事务模块
  ASSERT(entry.module().id() == LogModule::Id::TRANSACTION, "invalid log module id: %d", entry.module().id());

  // 检查日志条目的大小是否足够容纳事务日志头
  if (entry.payload_size() < MvccTrxLogHeader::SIZE) {
    LOG_WARN("invalid log entry size: %d, trx log header size:%ld", entry.payload_size(), MvccTrxLogHeader::SIZE);
    return RC::LOG_ENTRY_INVALID;
  }

  // 解析事务日志头
  auto *header = reinterpret_cast<const MvccTrxLogHeader *>(entry.data());
  MvccTrx *trx = nullptr;

  // 查找或创建事务对象
  auto trx_iter = trx_map_.find(header->trx_id);
  if (trx_iter == trx_map_.end()) {
    trx = static_cast<MvccTrx *>(trx_kit_.create_trx(log_handler_, header->trx_id));
    // trx = new MvccTrx(trx_kit_, log_handler_, header->trx_id);
  } else {
    trx = trx_iter->second;
  }

  /// 直接调用事务代码自己的重放函数
  rc = trx->redo(&db_, entry);

  /// 如果事务结束了，需要从内存中把它删除
  if (MvccTrxLogOperation(header->operation_type).type() == MvccTrxLogOperation::Type::ROLLBACK ||
      MvccTrxLogOperation(header->operation_type).type() == MvccTrxLogOperation::Type::COMMIT) {
    Trx *trx = trx_map_[header->trx_id];
    trx_kit_.destroy_trx(trx);
    trx_map_.erase(header->trx_id);
  }

  return rc;
}

/**
 * 当日志回放完成后调用此函数进行清理工作
 * 
 * 此函数的主要职责是处理在日志回放过程中未提交的事务这些事务需要被回滚以保持数据一致性
 * 同时，为了资源回收，还需要删除事务对象并清空事务映射表
 * 
 * @return RC 表示函数执行结果的状态码
 */
RC MvccTrxLogReplayer::on_done()
{
  /// 日志回放已经完成，需要把没有提交的事务，回滚掉
  for (auto &pair : trx_map_) {
    MvccTrx *trx = pair.second;
    trx->rollback(); // 恢复时的rollback，可能遇到之前已经回滚一半的事务又再次调用回滚的情况
    delete pair.second;
  }
  trx_map_.clear();

  return RC::SUCCESS;
}
