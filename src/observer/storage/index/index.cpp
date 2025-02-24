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
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/index.h"

/**
 * 初始化索引对象
 * 
 * @param index_meta 索引的元数据，包含了索引的相关信息和配置
 * @param field_meta 字段的元数据，包含了字段的相关信息和配置
 * 
 * @return RC::SUCCESS 表示初始化成功
 * 
 * 本函数通过接收索引和字段的元数据来初始化索引对象，使其能够根据指定的配置进行工作
 */
RC Index::init(const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  index_meta_ = index_meta;
  field_meta_ = field_meta;
  return RC::SUCCESS;
}
