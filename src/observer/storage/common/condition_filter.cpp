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
// Created by Wangyunlai on 2021/5/7.
//

#include "condition_filter.h"
#include "common/log/log.h"
#include "common/value.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include <math.h>
#include <stddef.h>

using namespace common;

// 默认条件过滤器的析构函数
ConditionFilter::~ConditionFilter() {}

// 默认条件过滤器的构造函数
DefaultConditionFilter::DefaultConditionFilter()
{
  // 初始化左操作数
  left_.is_attr     = false; // 是否为属性
  left_.attr_length = 0;      // 属性长度
  left_.attr_offset = 0;      // 属性偏移

  // 初始化右操作数
  right_.is_attr     = false; // 是否为属性
  right_.attr_length = 0;      // 属性长度
  right_.attr_offset = 0;      // 属性偏移
}

// 默认条件过滤器的析构函数
DefaultConditionFilter::~DefaultConditionFilter() {}

// 初始化条件过滤器
RC DefaultConditionFilter::init(const ConDesc &left, const ConDesc &right, AttrType attr_type, CompOp comp_op)
{
  // 校验属性类型
  if (attr_type <= AttrType::UNDEFINED || attr_type >= AttrType::MAXTYPE) {
    LOG_ERROR("Invalid condition with unsupported attribute type: %d", attr_type);
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  // 校验比较操作符
  if (comp_op < EQUAL_TO || comp_op >= NO_OP) {
    LOG_ERROR("Invalid condition with unsupported compare operation: %d", comp_op);
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  // 设置左侧和右侧条件
  left_      = left;
  right_     = right;
  attr_type_ = attr_type;
  comp_op_   = comp_op;
  return RC::SUCCESS; // 返回成功
}

// 从条件 SQL 节点初始化条件过滤器
RC DefaultConditionFilter::init(Table &table, const ConditionSqlNode &condition)
{
  const TableMeta &table_meta = table.table_meta(); // 获取表的元数据
  ConDesc          left; // 左侧条件描述
  ConDesc          right; // 右侧条件描述

  AttrType type_left  = AttrType::UNDEFINED; // 左侧属性类型
  AttrType type_right = AttrType::UNDEFINED; // 右侧属性类型

  // 处理左侧条件
  if (1 == condition.left_is_attr) {
    left.is_attr                = true; // 设置为属性
    const FieldMeta *field_left = table_meta.field(condition.left_attr.attribute_name.c_str());
    if (nullptr == field_left) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.left_attr.attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING; // 返回缺少字段错误
    }
    left.attr_length = field_left->len(); // 设置属性长度
    left.attr_offset = field_left->offset(); // 设置属性偏移

    type_left = field_left->type(); // 获取属性类型
  } else {
    left.is_attr = false; // 设置为常量
    left.value   = condition.left_value;  // 设置左侧值
    type_left    = condition.left_value.attr_type(); // 获取属性类型

    left.attr_length = 0; // 常量属性长度为 0
    left.attr_offset = 0; // 常量属性偏移为 0
  }

  // 处理右侧条件
  if (1 == condition.right_is_attr) {
    right.is_attr                = true; // 设置为属性
    const FieldMeta *field_right = table_meta.field(condition.right_attr.attribute_name.c_str());
    if (nullptr == field_right) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.right_attr.attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING; // 返回缺少字段错误
    }
    right.attr_length = field_right->len(); // 设置属性长度
    right.attr_offset = field_right->offset(); // 设置属性偏移
    type_right        = field_right->type(); // 获取属性类型
  } else {
    right.is_attr = false; // 设置为常量
    right.value   = condition.right_value; // 设置右侧值
    type_right    = condition.right_value.attr_type(); // 获取属性类型

    right.attr_length = 0; // 常量属性长度为 0
    right.attr_offset = 0; // 常量属性偏移为 0
  }

  // 校验和转换属性类型
  if (type_left != type_right) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH; // 返回字段类型不匹配错误
  }

  return init(left, right, type_left, condition.comp); // 初始化过滤器
}

// 过滤记录
bool DefaultConditionFilter::filter(const Record &rec) const
{
  Value left_value; // 左侧值
  Value right_value; // 右侧值

  // 处理左侧值
  if (left_.is_attr) {  // 如果是属性
    left_value.set_type(attr_type_); // 设置属性类型
    left_value.set_data(rec.data() + left_.attr_offset, left_.attr_length); // 设置属性数据
  } else {
    left_value.set_value(left_.value); // 设置常量值
  }

  // 处理右侧值
  if (right_.is_attr) { // 如果是属性
    right_value.set_type(attr_type_); // 设置属性类型
    right_value.set_data(rec.data() + right_.attr_offset, right_.attr_length); // 设置属性数据
  } else {
    right_value.set_value(right_.value); // 设置常量值
  }

  int cmp_result = left_value.compare(right_value); // 比较左侧和右侧值

  // 根据比较结果和操作符返回结果
  switch (comp_op_) {
    case EQUAL_TO: return 0 == cmp_result; // 相等
    case LESS_EQUAL: return cmp_result <= 0; // 小于或等于
    case NOT_EQUAL: return cmp_result != 0; // 不相等
    case LESS_THAN: return cmp_result < 0; // 小于
    case GREAT_EQUAL: return cmp_result >= 0; // 大于或等于
    case GREAT_THAN: return cmp_result > 0; // 大于

    default: break; // 默认情况
  }

  LOG_PANIC("Never should print this."); // 如果到达这里，表示错误
  return cmp_result;  // 不应该到达这里
}

// 复合条件过滤器的析构函数
CompositeConditionFilter::~CompositeConditionFilter()
{
  if (memory_owner_) {
    delete[] filters_; // 释放内存
    filters_ = nullptr;
  }
}

// 初始化复合条件过滤器
RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num, bool own_memory)
{
  filters_      = filters; // 设置过滤器数组
  filter_num_   = filter_num; // 设置过滤器数量
  memory_owner_ = own_memory; // 设置内存拥有权
  return RC::SUCCESS; // 返回成功
}

// 仅初始化复合条件过滤器（不拥有内存）
RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num)
{
  return init(filters, filter_num, false); // 默认不拥有内存
}

// 从条件 SQL 节点初始化复合条件过滤器
RC CompositeConditionFilter::init(Table &table, const ConditionSqlNode *conditions, int condition_num)
{
  if (condition_num == 0) {
    return RC::SUCCESS; // 如果没有条件，直接返回成功
  }
  if (conditions == nullptr) {
    return RC::INVALID_ARGUMENT; // 返回无效参数错误
  }

  RC                rc                = RC::SUCCESS; // 初始化返回值
  ConditionFilter **condition_filters = new ConditionFilter *[condition_num]; // 创建过滤器数组
  for (int i = 0; i < condition_num; i++) {
    DefaultConditionFilter *default_condition_filter = new DefaultConditionFilter(); // 创建默认过滤器
    rc                                               = default_condition_filter->init(table, conditions[i]); // 初始化
    if (rc != RC::SUCCESS) { // 如果初始化失败
      delete default_condition_filter; // 释放过滤器内存
      for (int j = i - 1; j >= 0; j--) {
        delete condition_filters[j]; // 释放已分配的过滤器内存
        condition_filters[j] = nullptr;
      }
      delete[] condition_filters; // 释放过滤器数组
      condition_filters = nullptr;
      return rc; // 返回错误
    }
    condition_filters[i] = default_condition_filter; // 保存过滤器
  }
  return init((const ConditionFilter **)condition_filters, condition_num, true); // 初始化复合过滤器
}

// 过滤记录
bool CompositeConditionFilter::filter(const Record &rec) const
{
  for (int i = 0; i < filter_num_; i++) { // 遍历所有过滤器
    if (!filters_[i]->filter(rec)) { // 如果有一个过滤器不通过
      return false; // 返回 false
    }
  }
  return true; // 所有过滤器通过，返回 true
}

