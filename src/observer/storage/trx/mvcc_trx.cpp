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
// Created by Wangyunlai on 2023/04/24.
//

#include "storage/trx/mvcc_trx.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include "storage/trx/mvcc_trx_log.h"
#include "common/lang/algorithm.h"

MvccTrxKit::~MvccTrxKit()
{
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);

  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxKit::init()
{
  // 事务使用一些特殊的字段，放到每行记录中，表示行记录的可见性。
  fields_ = vector<FieldMeta>{
      // field_id in trx fields is invisible.
      FieldMeta("__trx_xid_begin", AttrType::INTS, 0 /*attr_offset*/, 4 /*attr_len*/, false /*visible*/, -1/*field_id*/),
      FieldMeta("__trx_xid_end", AttrType::INTS, 0 /*attr_offset*/, 4 /*attr_len*/, false /*visible*/, -2/*field_id*/)};

  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxKit::trx_fields() const { return &fields_; }

int32_t MvccTrxKit::next_trx_id() { return ++current_trx_id_; }

int32_t MvccTrxKit::max_trx_id() const { return numeric_limits<int32_t>::max(); }

Trx *MvccTrxKit::create_trx(LogHandler &log_handler)
{
  Trx *trx = new MvccTrx(*this, log_handler);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

/**
 * 创建一个新的事务对象并更新事务列表和当前事务ID。
 * 
 * 此函数负责生成一个新的事务对象（Trx），并将它添加到事务列表中，
 * 同时检查并更新当前最大的事务ID。这确保了事务的唯一性和有序性。
 * 
 * @param log_handler 日志处理器引用，用于记录事务日志。
 * @param trx_id 新事务的ID。
 * @return 返回新创建的事务对象指针，如果创建失败则返回nullptr。
 */
Trx *MvccTrxKit::create_trx(LogHandler &log_handler, int32_t trx_id)
{
  // 创建一个新的MvccTrx事务对象。
  Trx *trx = new MvccTrx(*this, log_handler, trx_id);
  if (trx != nullptr) {
    // 锁定以确保线程安全，在操作共享资源前加锁是必要的。
    lock_.lock();
    // 将新创建的事务添加到事务列表中。
    trxes_.push_back(trx);
    // 如果当前事务ID小于新事务ID，则更新当前事务ID。
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    // 解锁以释放资源，确保其他线程可以继续执行。
    lock_.unlock();
  }
  // 返回新创建的事务对象指针。
  return trx;
}

void MvccTrxKit::destroy_trx(Trx *trx)
{
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend; ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();

  delete trx;
}

/**
 * 在MvccTrxKit类中查找事务
 * 
 * 此函数通过事务ID来查找对应的事务对象如果找到匹配的事务ID，则返回该事务对象；
 * 如果没有找到，则返回nullptr此函数体现了多版本并发控制(MVCC)中对事务管理的需求
 * 
 * @param trx_id 需要查找的事务ID
 * @return 如果找到匹配的事务，则返回事务对象指针；否则返回nullptr
 */
Trx *MvccTrxKit::find_trx(int32_t trx_id)
{
  // 加锁以确保线程安全
  lock_.lock();
  // 遍历事务列表，寻找匹配的事务ID
  for (Trx *trx : trxes_) {
    // 如果找到匹配的事务ID，解锁并返回该事务对象
    if (trx->id() == trx_id) {
      lock_.unlock();
      return trx;
    }
  }
  // 如果没有找到匹配的事务ID，解锁并返回nullptr
  lock_.unlock();
  return nullptr;
}

void MvccTrxKit::all_trxes(vector<Trx *> &trxes)
{
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

LogReplayer *MvccTrxKit::create_log_replayer(Db &db, LogHandler &log_handler)
{
  return new MvccTrxLogReplayer(db, *this, log_handler);
}

////////////////////////////////////////////////////////////////////////////////

MvccTrx::MvccTrx(MvccTrxKit &kit, LogHandler &log_handler) : trx_kit_(kit), log_handler_(log_handler)
{}

MvccTrx::MvccTrx(MvccTrxKit &kit, LogHandler &log_handler, int32_t trx_id) 
  : trx_kit_(kit), log_handler_(log_handler), trx_id_(trx_id)
{
  started_    = true;
  recovering_ = true;
}

MvccTrx::~MvccTrx() {}

/**
 * 在多版本并发控制事务中插入记录
 * 
 * 此函数负责在给定的表中插入一条记录，并处理与事务相关的元数据它通过设置记录的开始和结束字段来维护事务的可见性规则，
 * 并确保记录的插入操作被正确地记录在日志中以便于后续的恢复操作
 * 
 * @param table 指向要插入记录的表的指针
 * @param record 要插入的记录
 * @return 返回插入操作的结果，如果成功则为RC::SUCCESS，否则为相应的错误代码
 */
RC MvccTrx::insert_record(Table *table, Record &record)
{
  // 定义记录的开始和结束字段，用于存储事务的标识信息
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  // 设置记录的开始字段为当前事务的负ID，表示记录的创建时间
  begin_field.set_int(record, -trx_id_);
  // 设置记录的结束字段为当前事务系统中的最大事务ID，表示记录的过期时间
  end_field.set_int(record, trx_kit_.max_trx_id());

  // 调用表的插入记录函数，尝试将记录插入到表中
  RC rc = table->insert_record(record);
  // 如果插入失败，记录错误日志并返回错误代码
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record into table. rc=%s", strrc(rc));
    return rc;
  }

  // 调用日志处理器的插入记录函数，尝试将插入操作记录到日志中
  rc = log_handler_.insert_record(trx_id_, table, record.rid());
  // 确保日志记录成功，否则抛出异常
  ASSERT(rc == RC::SUCCESS, "failed to append insert record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
         trx_id_, table->table_id(), record.rid().to_string().c_str(), record.len(), strrc(rc));

  // 将插入操作添加到事务的操作列表中，以便于事务的回滚或提交
  operations_.push_back(Operation(Operation::Type::INSERT, table, record.rid()));
  // 返回日志记录的结果，如果成功则为RC::SUCCESS，否则为相应的错误代码
  return rc;
}

// 删除指定表中的记录
// 参数:
//   table: 表指针，表示要删除记录的表
//   record: 记录对象，表示要删除的记录
// 返回值:
//   RC: 返回删除操作的结果，成功或失败
RC MvccTrx::delete_record(Table *table, Record &record)
{
  // 定义开始和结束字段，用于后续操作
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  // 初始化删除结果为成功
  RC delete_result = RC::SUCCESS;

  // 遍历记录，执行删除操作
  RC rc = table->visit_record(record.rid(), [this, table, &delete_result, &end_field](Record &inplace_record) -> bool {
    // 访问记录，进行读写操作
    RC rc = this->visit_record(table, inplace_record, ReadWriteMode::READ_WRITE);
    if (OB_FAIL(rc)) {
      delete_result = rc;
      return false;
    }

    // 设置记录的结束字段为当前事务ID的负值，表示记录被删除
    end_field.set_int(inplace_record, -trx_id_);
    return true;
  });

  // 如果遍历记录失败，返回错误
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to visit record. rc=%s", strrc(rc));
    return rc;
  }

  // 如果删除操作失败，返回错误
  if (OB_FAIL(delete_result)) {
    LOG_TRACE("record is not visible. rid=%s, rc=%s", record.rid().to_string().c_str(), strrc(delete_result));
    return delete_result;
  }

  // 记录删除操作到日志
  rc = log_handler_.delete_record(trx_id_, table, record.rid());
  ASSERT(rc == RC::SUCCESS, "failed to append delete record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_, table->table_id(), record.rid().to_string().c_str(), record.len(), strrc(rc));

  // 将删除操作添加到操作列表
  operations_.push_back(Operation(Operation::Type::DELETE, table, record.rid()));

  // 返回成功
  return RC::SUCCESS;
}

/**
 * @brief 访问记录并确定其可见性
 * 
 * 本函数根据事务id和记录的开始、结束事务id来判断记录对当前事务是否可见
 * 它处理了记录插入、删除以及并发事务对记录可见性的影响
 * 
 * @param table 表对象，用于获取记录的字段信息
 * @param record 记录对象，包含开始和结束事务id
 * @param mode 事务的读写模式，影响对未提交删除记录的处理
 * @return RC 返回访问结果，包括成功、记录不可见和并发冲突等情况
 */
RC MvccTrx::visit_record(Table *table, Record &record, ReadWriteMode mode)
{
  // 初始化记录的开始和结束字段
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  // 获取记录的开始和结束事务id
  int32_t begin_xid = begin_field.get_int(record);
  int32_t end_xid   = end_field.get_int(record);

  RC rc = RC::SUCCESS;

  // 处理记录的可见性判断
  if (begin_xid > 0 && end_xid > 0) {
    // 如果事务id在记录的开始和结束事务id之间，记录可见
    if (trx_id_ >= begin_xid && trx_id_ <= end_xid) {
      rc = RC::SUCCESS;
    } else {
      // 记录不可见，事务id不在记录的可见范围内
      LOG_TRACE("record invisible. trx id=%d, begin xid=%d, end xid=%d", trx_id_, begin_xid, end_xid);
      rc = RC::RECORD_INVISIBLE;
    }
  } else if (begin_xid < 0) {
    // 开始事务id小于0表示记录是未提交的插入
    if (-begin_xid == trx_id_) {
      rc = RC::SUCCESS;
    } else {
      // 记录不可见，有其他事务正在插入此记录
      LOG_TRACE("record invisible. someone is updating this record right now. trx id=%d, begin xid=%d, end xid=%d",
                trx_id_, begin_xid, end_xid);
      rc = RC::RECORD_INVISIBLE;
    }
  } else if (end_xid < 0) {
    // 结束事务id小于0表示记录是未提交的删除
    if (mode == ReadWriteMode::READ_ONLY) {
      // 读事务处理未提交删除的记录
      if (-end_xid != trx_id_) {
        rc = RC::SUCCESS;
      } else {
        // 记录不可见，当前事务已删除此记录
        LOG_TRACE("record invisible. self has deleted this record. trx id=%d, begin xid=%d, end xid=%d",
                  trx_id_, begin_xid, end_xid);
        rc = RC::RECORD_INVISIBLE;
      }
    } else {
      // 写事务处理未提交删除的记录
      if (-end_xid != trx_id_) {
        // 并发冲突，有其他事务正在删除此记录
        LOG_TRACE("concurrency conflit. someone is deleting this record right now. trx id=%d, begin xid=%d, end xid=%d",
                  trx_id_, begin_xid, end_xid);
        rc = RC::LOCKED_CONCURRENCY_CONFLICT;
      } else {
        // 记录不可见，当前事务已删除此记录
        LOG_TRACE("record invisible. self has deleted this record. trx id=%d, begin xid=%d, end xid=%d",
                  trx_id_, begin_xid, end_xid);
        rc = RC::RECORD_INVISIBLE;
      }
    }
  }
  return rc;
}

/**
 * @brief 获取指定表上的事务使用的字段
 *
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const
{
  const TableMeta      &table_meta = table->table_meta();
  span<const FieldMeta> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.size() >= 2, "invalid trx fields number. %d", trx_fields.size());

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields[1]);
}

RC MvccTrx::start_if_need()
{
  if (!started_) {
    ASSERT(operations_.empty(), "try to start a new trx while operations is not empty");
    trx_id_ = trx_kit_.next_trx_id();
    LOG_DEBUG("current thread change to new trx with %d", trx_id_);
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit()
{
  int32_t commit_id = trx_kit_.next_trx_id();
  return commit_with_trx_id(commit_id);
}

// 提交事务并指定事务ID
// 该函数负责将当前事务的所有操作应用到数据库中，并标记事务为已提交
// 主要处理INSERT和DELETE操作，更新记录的事务ID字段以维护多版本控制
RC MvccTrx::commit_with_trx_id(int32_t commit_xid)
{
  // TODO 原子性提交BUG：这里存在一个很大的问题，不能让其他事务一次性看到当前事务更新到的数据或同时看不到
  RC rc    = RC::SUCCESS;
  started_ = false;

  // 遍历事务中的所有操作并提交
  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        // 处理INSERT操作
        RID    rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field  begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        // 更新记录的开始事务ID为提交事务ID
        auto record_updater = [this, &begin_xid_field, commit_xid](Record &record) -> bool {
          LOG_DEBUG("before commit insert record. trx id=%d, begin xid=%d, commit xid=%d, lbt=%s",
                    trx_id_, begin_xid_field.get_int(record), commit_xid, lbt());
          ASSERT(begin_xid_field.get_int(record) == -this->trx_id_ && (!recovering_), 
                 "got an invalid record while committing. begin xid=%d, this trx id=%d", 
                 begin_xid_field.get_int(record), trx_id_);

          begin_xid_field.set_int(record, commit_xid);
          return true;
        };

        // 应用更新到记录
        rc = operation.table()->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        // 处理DELETE操作
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        // 更新记录的结束事务ID为提交事务ID
        auto record_updater = [this, &end_xid_field, commit_xid](Record &record) -> bool {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_, 
                 "got an invalid record while committing. end xid=%d, this trx id=%d", 
                 end_xid_field.get_int(record), trx_id_);

          end_xid_field.set_int(record, commit_xid);
          return true;
        };

        // 应用更新到记录
        rc = operation.table()->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        // 不支持的操作类型
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  // 如果不是在恢复过程中，提交日志
  if (!recovering_) {
    rc = log_handler_.commit(trx_id_, commit_xid);
  }

  // 清空操作列表
  operations_.clear();

  // 日志记录事务提交
  LOG_TRACE("append trx commit log. trx id=%d, commit_xid=%d, rc=%s", trx_id_, commit_xid, strrc(rc));
  return rc;
}

// 回滚事务
// 该函数将事务标记为未开始，并逐个回滚事务中的操作
RC MvccTrx::rollback()
{
  RC rc    = RC::SUCCESS;
  started_ = false;

  // 逆序遍历事务操作，因为事务的回滚需要按照操作的逆序进行
  for (auto iter = operations_.rbegin(), itend = operations_.rend(); iter != itend; ++iter) {
    const Operation &operation = *iter;
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        // 回滚插入操作，通过删除记录实现
        RID    rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        // 这里也可以不删除，仅仅给数据加个标识位，等垃圾回收器来收割也行

        if (recovering_) {
          // 恢复的时候，需要额外判断下当前记录是否还是当前事务拥有。是的话才能删除记录
          Record record;
          rc = table->get_record(rid, record);
          if (OB_SUCC(rc)) {
            Field begin_xid_field, end_xid_field;
            trx_fields(table, begin_xid_field, end_xid_field);
            if (begin_xid_field.get_int(record) != -trx_id_) {
              continue;
            }
          } else if (RC::RECORD_NOT_EXIST == rc) {
            continue;
          } else {
            LOG_WARN("failed to get record while rollback. table=%s, rid=%s, rc=%s", 
                     table->name(), rid.to_string().c_str(), strrc(rc));
            return rc;
          }
        }
        rc = table->delete_record(rid);
        ASSERT(rc == RC::SUCCESS, "failed to delete record while rollback. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        // 回滚删除操作，通过更新记录的事务id字段实现
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s",
              rid.to_string().c_str(), strrc(rc));
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        // 定义一个记录更新器，用于更新记录的结束事务id字段
        auto record_updater = [this, &end_xid_field](Record &record) -> bool {
          if (recovering_ && end_xid_field.get_int(record) != -trx_id_) {
            return false;
          }

          ASSERT(end_xid_field.get_int(record) == -trx_id_, 
                "got an invalid record while rollback. end xid=%d, this trx id=%d", 
                end_xid_field.get_int(record), trx_id_);

          end_xid_field.set_int(record, trx_kit_.max_trx_id());
          return true;
        };

        rc = table->visit_record(rid, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        // 遇到不支持的操作类型时抛出断言错误
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  // 清空事务操作列表
  operations_.clear();

  // 如果不是在恢复模式下，调用日志处理器回滚事务
  if (!recovering_) {
    rc = log_handler_.rollback(trx_id_);
  }
  // 记录事务回滚日志
  LOG_TRACE("append trx rollback log. trx id=%d, rc=%s", trx_id_, strrc(rc));
  return rc;
}

/**
 * 根据日志条目查找表
 * 
 * @param db 数据库指针，用于查找表
 * @param log_entry 日志条目，包含查找表所需的信息
 * @param table 输出参数，用于存储找到的表指针
 * @return 返回码，表示执行结果
 * 
 * 本函数根据日志条目的类型和信息，查找并返回相应的表指针
 * 如果日志条目表示插入或删除记录的操作，则解析日志条目中的表ID，
 * 并使用数据库指针查找对应的表如果表不存在，则返回错误码
 * 对于其他类型的日志条目，不执行任何操作
 */
RC find_table(Db *db, const LogEntry &log_entry, Table *&table)
{
  // 解释日志条目的头部信息，以获取操作类型
  auto *trx_log_header = reinterpret_cast<const MvccTrxLogHeader *>(log_entry.data());
  switch (MvccTrxLogOperation(trx_log_header->operation_type).type()) {
    case MvccTrxLogOperation::Type::INSERT_RECORD:
    case MvccTrxLogOperation::Type::DELETE_RECORD: {
      // 解释日志条目中的记录信息，以获取表ID
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      table                = db->find_table(trx_log_record->table_id);
      if (nullptr == table) {
        // 如果表不存在，记录警告日志并返回错误码
        LOG_WARN("no such table to redo. log record=%s", trx_log_record->to_string().c_str());
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
    } break;
    default: {
      // 对于其他操作类型，不执行任何操作
    } break;
  }
  // 成功执行，返回成功码
  return RC::SUCCESS;
}

/**
 * 执行MVCC事务的日志重做操作
 * 
 * @param db 数据库对象指针，用于访问数据库相关操作
 * @param log_entry 日志条目，包含需要重做的操作信息
 * 
 * @return RC 返回执行结果，成功为RC::SUCCESS，失败为其他错误码
 * 
 * 此函数根据日志条目中的操作类型，执行相应的插入、删除、提交或回滚操作
 * 它是事务恢复过程中重做日志的关键步骤
 */
RC MvccTrx::redo(Db *db, const LogEntry &log_entry)
{
  // 解释日志条目数据为事务日志头结构
  auto *trx_log_header = reinterpret_cast<const MvccTrxLogHeader *>(log_entry.data());
  Table *table = nullptr;
  RC     rc    = find_table(db, log_entry, table);
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 根据事务日志操作类型执行相应的操作
  switch (MvccTrxLogOperation(trx_log_header->operation_type).type()) {
    case MvccTrxLogOperation::Type::INSERT_RECORD: {
      // 解释日志条目数据为记录操作结构，并添加插入操作到操作列表
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      operations_.push_back(Operation(Operation::Type::INSERT, table, trx_log_record->rid));
    } break;

    case MvccTrxLogOperation::Type::DELETE_RECORD: {
      // 解释日志条目数据为记录操作结构，并添加删除操作到操作列表
      auto *trx_log_record = reinterpret_cast<const MvccTrxRecordLogEntry *>(log_entry.data());
      operations_.push_back(Operation(Operation::Type::DELETE, table, trx_log_record->rid));
    } break;

    case MvccTrxLogOperation::Type::COMMIT: {
      // 遇到了提交日志，说明前面的记录都已经提交成功了
      // 此处可添加提交事务的逻辑，例如更新事务状态
    } break;

    case MvccTrxLogOperation::Type::ROLLBACK: {
      // 遇到了回滚日志，前面的回滚操作也都执行完成了
      // 此处可添加回滚事务的逻辑，例如释放资源
    } break;

    default: {
      // 如果遇到不支持的日志类型，则返回内部错误
      ASSERT(false, "unsupported redo log. log_record=%s", log_entry.to_string().c_str());
      return RC::INTERNAL;
    } break;
  }

  // 日志重做操作成功完成
  return RC::SUCCESS;
}
