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
// Created by Wangyunlai on 2023/4/24.
//

#include "storage/trx/vacuous_trx.h"

RC VacuousTrxKit::init() { return RC::SUCCESS; }

const vector<FieldMeta> *VacuousTrxKit::trx_fields() const { return nullptr; }

Trx *VacuousTrxKit::create_trx(LogHandler &) { return new VacuousTrx; }

Trx *VacuousTrxKit::create_trx(LogHandler &, int32_t /*trx_id*/) { return nullptr; }

void VacuousTrxKit::destroy_trx(Trx *trx) { delete trx; }

Trx *VacuousTrxKit::find_trx(int32_t /* trx_id */) { return nullptr; }

void VacuousTrxKit::all_trxes(vector<Trx *> &trxes) { return; }

LogReplayer *VacuousTrxKit::create_log_replayer(Db &, LogHandler &) { return new VacuousTrxLogReplayer; }

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief 在空事务中插入记录
 * 
 * 此函数将记录插入到指定的表中。它简单地调用表的insert_record方法来执行插入操作。
 * 由于这个类代表一个空事务，所以这个函数并不处理任何事务逻辑，只是将插入操作委托给表对象。
 * 
 * @param table 指向要插入记录的表的指针
 * @param record 要插入的记录
 * @return RC 插入操作的结果代码，表示操作是否成功
 */
RC VacuousTrx::insert_record(Table *table, Record &record) { return table->insert_record(record); }

/**
 * @brief 删除记录
 * 
 * 该函数委托表对象删除给定的记录。它直接调用表对象的delete_record方法，
 * 并返回执行结果。此函数体现了事务在删除记录时的委托行为。
 * 
 * @param table 指向要操作的表的对象指针
 * @param record 引用了要删除的记录
 * @return RC 返回删除操作的结果，RC为结果代码类型
 */
RC VacuousTrx::delete_record(Table *table, Record &record) { return table->delete_record(record); }

/**
 * @brief 对一个表中的记录进行空操作事务访问
 * 
 * 本函数实现了一个空操作事务的记录访问它不进行任何实际的操作，通常用于测试或特定的场景模拟
 * 由于其空操作的特性，此函数总是成功返回，不会产生任何副作用
 * 
 * @param table 指向将要访问的表的对象指针
 * @param record 引用将要访问的记录对象
 * @param mode 读写模式，指示事务的类型，尽管在这个空操作事务中未使用
 * @return RC 总是返回RC::SUCCESS，表示操作成功
 */
RC VacuousTrx::visit_record(Table *table, Record &record, ReadWriteMode) { return RC::SUCCESS; }

/**
 * @brief 有条件地启动事务
 * 
 * 该函数检查是否需要启动事务。如果不需要执行任何操作，例如当事务已经处于活动状态或特定条件下不需要事务时。
 * 这样设计是为了避免重复启动事务或在不必要的情况下启动事务，从而提高效率和避免潜在的并发问题。
 * 
 * @return RC 返回成功状态码，表示事务启动成功或无需启动事务。
 */
RC VacuousTrx::start_if_need() { return RC::SUCCESS; }

/**
 * @brief Commits a vacuous transaction.
 * 
 * A vacuous transaction is a special type of transaction that does not perform any actual operations,
 * so its commit operation is also special. This function does not perform any operations,
 * but directly returns success.
 * 
 * @return RC Returns RC::SUCCESS to indicate successful commit.
 */
RC VacuousTrx::commit() { return RC::SUCCESS; }

/**
 * @brief 回滚一个空事务
 * 
 * 该函数回滚一个空事务。由于该事务没有执行任何操作，因此该函数实际上什么都不做。
 * 它返回成功状态码RC::SUCCESS，表示回滚操作成功完成，尽管实际上并没有进行任何回滚操作。
 * 
 * @return RC 始终返回RC::SUCCESS，表示回滚操作成功
 */
RC VacuousTrx::rollback() { return RC::SUCCESS; }

/**
 * @brief 执行重做操作的函数
 * 
 * 本函数用于执行数据库的重做操作，但在当前实现中，它不执行任何实际操作，
 * 只是简单地返回成功状态。这通常用于不需要重做操作的事务处理场景。
 * 
 * @param db 指向数据库的指针，用于执行重做操作
 * @param logEntry 日志条目，包含重做操作所需的信息
 * @return RC 返回操作状态，总是返回成功状态
 */
RC VacuousTrx::redo(Db *, const LogEntry &) { return RC::SUCCESS; }
