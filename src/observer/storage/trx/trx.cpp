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
// Created by Wangyunlai on 2021/5/24.
//

#include <atomic>

#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field.h"
#include "storage/field/field_meta.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/trx/mvcc_trx.h"
#include "storage/trx/trx.h"
#include "storage/trx/vacuous_trx.h"

/**
 * 创建并初始化一个事务套件(TrxKit)对象。
 * 
 * 根据指定的名称选择并创建相应类型的TrxKit对象。如果名称为空或为"vacuous"，
 * 则创建一个VacuousTrxKit对象；如果名称为"mvcc"，则创建一个MvccTrxKit对象。
 * 否则，记录错误日志并返回nullptr。创建对象后，将调用init方法进行初始化。
 * 如果初始化失败，将删除对象并返回nullptr。
 * 
 * @param name 事务套件的名称，用于选择创建哪种类型的TrxKit对象。
 * @return 返回创建的TrxKit对象指针，如果创建或初始化失败，则返回nullptr。
 */
TrxKit *TrxKit::create(const char *name)
{
  // 初始化事务套件指针为nullptr
  TrxKit *trx_kit = nullptr;

  // 根据名称选择并创建相应的TrxKit对象
  if (common::is_blank(name) || 0 == strcasecmp(name, "vacuous")) {
    trx_kit = new VacuousTrxKit();
  } else if (0 == strcasecmp(name, "mvcc")) {
    trx_kit = new MvccTrxKit();
  } else {
    // 如果名称不匹配已知类型，记录错误日志并返回nullptr
    LOG_ERROR("unknown trx kit name. name=%s", name);
    return nullptr;
  }

  // 调用新创建的TrxKit对象的init方法进行初始化
  RC rc = trx_kit->init();
  if (OB_FAIL(rc)) {
    // 如果初始化失败，记录错误日志，删除对象，并返回nullptr
    LOG_ERROR("failed to init trx kit. name=%s, rc=%s", name, strrc(rc));
    delete trx_kit;
    trx_kit = nullptr;
  }
  
  // 返回创建并成功初始化的TrxKit对象指针，或在失败时返回nullptr
  return trx_kit;
}
