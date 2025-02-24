/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// 作者：Meiyi & Wangyunlai 创建于 2021/5/13

#include <limits.h>
#include <string.h>

#include "common/defs.h"
#include "common/lang/string.h"
#include "common/lang/span.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/global_context.h"
#include "storage/db/db.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/index.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

Table::~Table() {
  // 析构函数，释放资源
  if (record_handler_ != nullptr) {
    delete record_handler_; // 删除记录处理器
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file(); // 关闭数据缓冲池文件
    data_buffer_pool_ = nullptr;
  }

  for (vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index; // 删除索引
  }
  indexes_.clear(); // 清空索引列表

  LOG_INFO("Table has been closed: %s", name()); // 记录表关闭信息
}

RC Table::create(Db *db, int32_t table_id, const char *path, const char *name, const char *base_dir,
    span<const AttrInfoSqlNode> attributes, StorageFormat storage_format) {
  // 创建表的方法
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT; // 无效的表ID
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT; // 表名不能为空
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name); // 开始创建表

  if (attributes.size() == 0) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d", name, attributes.size());
    return RC::INVALID_ARGUMENT; // 属性数量无效
  }

  RC rc = RC::SUCCESS;

  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST; // 表文件已存在
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN; // 打开文件失败
  }

  close(fd); // 关闭文件描述符

  // 创建文件
  const vector<FieldMeta> *trx_fields = db->trx_kit().trx_fields();
  if ((rc = table_meta_.init(table_id, name, trx_fields, attributes, storage_format)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // 初始化表元数据失败
  }

  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary); // 打开文件以写入
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN; // 打开文件失败
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs); // 序列化表元数据
  fs.close(); // 关闭文件

  db_       = db; // 设置数据库指针
  base_dir_ = base_dir; // 设置基础目录

  string data_file = table_data_file(base_dir, name); // 构建数据文件路径
  BufferPoolManager &bpm = db->buffer_pool_manager();
  rc = bpm.create_file(data_file.c_str()); // 创建数据文件
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc; // 创建数据文件失败
  }

  rc = init_record_handler(base_dir); // 初始化记录处理器
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
    // 不需要删除数据文件
    return rc;
  }

  LOG_INFO("Successfully create table %s:%s", base_dir, name); // 表创建成功
  return rc;
}

RC Table::open(Db *db, const char *meta_file, const char *base_dir) {
  // 打开表的方法
  fstream fs;
  string  meta_file_path = string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file; // 构建元数据文件路径
  fs.open(meta_file_path, ios_base::in | ios_base::binary); // 以只读方式打开元数据文件
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN; // 打开元数据文件失败
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL; // 反序列化元数据失败
  }
  fs.close(); // 关闭文件

  db_       = db; // 设置数据库指针
  base_dir_ = base_dir; // 设置基础目录

  // 加载数据文件
  RC rc = init_record_handler(base_dir); // 初始化记录处理器
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
    // 不需要删除数据文件
    return rc;
  }

  const int index_num = table_meta_.index_num(); // 获取索引数量
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i); // 获取索引元数据
    const FieldMeta *field_meta = table_meta_.field(index_meta->field()); // 获取字段元数据
    if (field_meta == nullptr) {
      LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                name(), index_meta->name(), index_meta->field());
      // 遇到无效索引元数据，跳过清理
      return RC::INTERNAL;
    }

    BplusTreeIndex *index = new BplusTreeIndex(); // 创建B+树索引
    string index_file = table_index_file(base_dir, name(), index_meta->name()); // 构建索引文件路径

    rc = index->open(this, index_file.c_str(), *index_meta, *field_meta); // 打开索引
    if (rc != RC::SUCCESS) {
      delete index; // 打开失败，删除索引
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                name(), index_meta->name(), index_file.c_str(), strrc(rc));
      return rc; // 返回错误
    }
    indexes_.push_back(index); // 将索引添加到列表
  }

  return rc;
}

RC Table::insert_record(Record &record) {
  // 插入记录的方法
  RC rc = RC::SUCCESS;
  rc = record_handler_->insert_record(record.data(), table_meta_.record_size(), &record.rid()); // 插入记录
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc; // 插入失败
  }

  rc = insert_entry_of_indexes(record.data(), record.rid()); // 插入索引条目
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), true); // 删除索引条目
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Delete entry of indexes failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc2));
      return rc2; // 删除失败
    }
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc; // 返回插入失败的错误
  }
  return rc; // 返回成功
}

RC Table::delete_entry_of_indexes(const char *data, const RID &rid, bool ignore_nonexist) {
  // 删除索引条目的方法
  RC rc = RC::SUCCESS;
  for (auto &index : indexes_) {
    rc = index->remove_entry(data, rid); // 从索引中删除条目
    if (rc != RC::SUCCESS && !ignore_nonexist) {
      return rc; // 如果发生错误且不忽略，则返回错误
    }
  }
  return rc; // 返回成功
}

RC Table::insert_entry_of_indexes(const char *data, const RID &rid) {
  // 插入索引条目的方法
  RC rc = RC::SUCCESS;
  for (auto &index : indexes_) {
    rc = index->insert_entry(data, rid); // 向索引中插入条目
    if (rc != RC::SUCCESS) {
      return rc; // 返回插入失败的错误
    }
  }
  return rc; // 返回成功
}


RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS; // 初始化返回状态
  // 遍历所有索引
  for (Index *index : indexes_) {
    // 从索引中删除条目
    rc = index->delete_entry(record, &rid);
    // 检查删除操作的结果
    if (rc != RC::SUCCESS) {
      // 如果删除失败且不是因为记录无效，或者不需要在不存在时返回错误，则退出循环
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc; // 返回操作结果
}

Index *Table::find_index(const char *index_name) const
{
  // 遍历所有索引
  for (Index *index : indexes_) {
    // 检查索引名称是否匹配
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index; // 找到索引，返回指针
    }
  }
  return nullptr; // 未找到索引，返回空指针
}

Index *Table::find_index_by_field(const char *field_name) const
{
  const TableMeta &table_meta = this->table_meta(); // 获取表元数据
  // 根据字段名称查找索引元数据
  const IndexMeta *index_meta = table_meta.find_index_by_field(field_name);
  if (index_meta != nullptr) {
    // 如果找到索引元数据，查找对应的索引
    return this->find_index(index_meta->name());
  }
  return nullptr; // 未找到索引，返回空指针
}

RC Table::sync()
{
  RC rc = RC::SUCCESS; // 初始化返回状态
  // 遍历所有索引并同步
  for (Index *index : indexes_) {
    rc = index->sync(); // 同步索引
    if (rc != RC::SUCCESS) {
      // 如果同步失败，记录错误信息
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc; // 返回错误状态
    }
  }

  // 同步数据缓冲池中的所有页面
  rc = data_buffer_pool_->flush_all_pages();
  LOG_INFO("Sync table over. table=%s", name()); // 记录同步完成的信息
  return rc; // 返回操作结果
}

