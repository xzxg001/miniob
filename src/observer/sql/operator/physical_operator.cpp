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
// Created by WangYunlai on 2022/11/18.
//

#include "sql/operator/physical_operator.h"  // 引入物理算子的定义

/**
 * @brief 将物理算子类型转换为字符串
 * @param type 物理算子类型
 * @return 返回对应的物理算子类型名称字符串
 */
std::string physical_operator_type_name(PhysicalOperatorType type)
{
  switch (type) {                                                            // 根据物理算子类型进行判断
    case PhysicalOperatorType::TABLE_SCAN: return "TABLE_SCAN";              // 表扫描
    case PhysicalOperatorType::INDEX_SCAN: return "INDEX_SCAN";              // 索引扫描
    case PhysicalOperatorType::NESTED_LOOP_JOIN: return "NESTED_LOOP_JOIN";  // 嵌套循环连接
    case PhysicalOperatorType::EXPLAIN: return "EXPLAIN";                    // 执行计划解释
    case PhysicalOperatorType::PREDICATE: return "PREDICATE";                // 谓词
    case PhysicalOperatorType::INSERT: return "INSERT";                      // 插入操作
    case PhysicalOperatorType::DELETE: return "DELETE";                      // 删除操作
    case PhysicalOperatorType::PROJECT: return "PROJECT";                    // 投影操作
    case PhysicalOperatorType::STRING_LIST: return "STRING_LIST";            // 字符串列表
    case PhysicalOperatorType::HASH_GROUP_BY: return "HASH_GROUP_BY";        // 哈希分组
    case PhysicalOperatorType::SCALAR_GROUP_BY: return "SCALAR_GROUP_BY";    // 标量分组
    case PhysicalOperatorType::AGGREGATE_VEC: return "AGGREGATE_VEC";        // 矢量化聚合
    case PhysicalOperatorType::GROUP_BY_VEC: return "GROUP_BY_VEC";          // 矢量化分组
    case PhysicalOperatorType::PROJECT_VEC: return "PROJECT_VEC";            // 矢量化投影
    case PhysicalOperatorType::TABLE_SCAN_VEC: return "TABLE_SCAN_VEC";      // 矢量化表扫描
    case PhysicalOperatorType::EXPR_VEC: return "EXPR_VEC";                  // 矢量化表达式
    default: return "UNKNOWN";                                               // 未知类型
  }
}

/**
 * @brief 获取物理算子的名称
 * @return 返回物理算子的名称
 */
std::string PhysicalOperator::name() const
{
  return physical_operator_type_name(type());  // 返回物理算子类型名称
}

/**
 * @brief 获取物理算子的参数
 * @return 返回物理算子的参数描述，默认为空字符串
 */
std::string PhysicalOperator::param() const
{
  return "";  // 返回物理算子的参数描述
}
