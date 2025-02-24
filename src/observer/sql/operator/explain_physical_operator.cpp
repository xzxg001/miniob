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
// Created by WangYunlai on 2022/12/27.
//

#include "sql/operator/explain_physical_operator.h"  // 包含解释物理算子的头文件
#include "common/log/log.h"                          // 包含日志记录的头文件
#include <sstream>                                   // 包含字符串流头文件，用于构建字符串
using namespace std;                                 // 使用标准命名空间

/**
 * @brief 打开解释物理算子，准备执行解释操作
 *
 * @param trx 指向当前事务的指针
 * @return RC 返回操作结果代码
 *
 * 此函数断言必须有一个子操作符，因为解释操作需要一个执行计划作为输入。
 */
RC ExplainPhysicalOperator::open(Trx *)
{
  ASSERT(children_.size() == 1, "explain must has 1 child");  // 确保只有一个子操作符
  return RC::SUCCESS;                                      // 返回成功
}

/**
 * @brief 关闭解释物理算子
 *
 * @return RC 返回操作结果代码
 *
 * 此函数在关闭物理算子时不需要额外处理，直接返回成功。
 */
RC ExplainPhysicalOperator::close() { return RC::SUCCESS; }

/**
 * @brief 生成物理执行计划的字符串表示
 *
 * 此函数会遍历当前操作符的子操作符，并将其信息以特定格式
 * 构建为一个字符串，便于打印和分析物理执行计划。
 */
void ExplainPhysicalOperator::generate_physical_plan()
{
  stringstream ss;           // 使用字符串流构建输出字符串
  ss << "OPERATOR(NAME)\n";  // 输出头部信息

  int               level = 0;  // 当前操作符层级
  std::vector<bool> ends;       // 用于表示当前层级的子操作符是否有兄弟节点
  ends.push_back(true);         // 初始化为有兄弟节点
  const auto children_size = static_cast<int>(children_.size());  // 获取子操作符的数量

  // 遍历子操作符
  for (int i = 0; i < children_size - 1; i++) {
    to_string(ss, children_[i].get(), level, false /*last_child*/, ends);  // 处理非最后子节点
  }
  if (children_size > 0) {
    to_string(ss, children_[children_size - 1].get(), level, true /*last_child*/, ends);  // 处理最后子节点
  }

  physical_plan_ = ss.str();  // 将构建的字符串赋值给物理计划成员变量
}

/**
 * @brief 获取下一条记录
 *
 * @return RC 返回操作结果代码
 *
 * 此函数会生成物理计划字符串，如果物理计划字符串不为空，
 * 则返回记录结束状态，表示没有更多记录可供返回。
 */
RC ExplainPhysicalOperator::next()
{
  if (!physical_plan_.empty()) {
    return RC::RECORD_EOF;  // 如果物理计划已生成，返回记录结束
  }
  generate_physical_plan();  // 生成物理计划

  vector<Value> cells;                         // 存储单元格的向量
  Value         cell(physical_plan_.c_str());  // 创建包含物理计划字符串的值
  cells.emplace_back(cell);                    // 将值添加到单元格向量中
  tuple_.set_cells(cells);                     // 设置元组的单元格
  return RC::SUCCESS;                          // 返回成功
}

/**
 * @brief 获取下一条记录，适用于 chunk
 *
 * @param chunk 存储获取的列数据
 * @return RC 返回操作结果代码
 *
 * 此函数与上一个 next 方法类似，不同之处在于它将结果添加到 chunk 中。
 */
RC ExplainPhysicalOperator::next(Chunk &chunk)
{
  if (!physical_plan_.empty()) {
    return RC::RECORD_EOF;  // 如果物理计划已生成，返回记录结束
  }
  generate_physical_plan();  // 生成物理计划

  Value cell(physical_plan_.c_str());      // 创建包含物理计划字符串的值
  auto  column = make_unique<Column>();    // 创建列对象
  column->init(cell);                      // 初始化列对象
  chunk.add_column(std::move(column), 0);  // 将列对象添加到 chunk 中
  return RC::SUCCESS;                      // 返回成功
}

/**
 * @brief 获取当前元组
 *
 * @return Tuple* 返回当前元组的指针
 *
 * 此函数返回当前操作符的元组，以便外部调用。
 */
Tuple *ExplainPhysicalOperator::current_tuple() { return &tuple_; }

/**
 * @brief 递归打印某个算子
 *
 * @param os 结果输出到这里
 * @param oper 将要打印的算子
 * @param level 当前算子在第几层
 * @param last_child 当前算子是否是当前兄弟节点中最后一个节点
 * @param ends 表示当前某个层级上的算子，是否已经没有其它的节点，以判断使用什么打印符号
 */
void ExplainPhysicalOperator::to_string(
    std::ostream &os, PhysicalOperator *oper, int level, bool last_child, std::vector<bool> &ends)
{
  for (int i = 0; i < level - 1; i++) {
    if (ends[i]) {
      os << "  ";  // 打印空格表示连接
    } else {
      os << "│ ";  // 打印竖线表示连接
    }
  }
  if (level > 0) {
    if (last_child) {
      os << "└─";              // 打印最后一个子节点符号
      ends[level - 1] = true;  // 更新ends数组
    } else {
      os << "├─";  // 打印非最后一个子节点符号
    }
  }

  os << oper->name();                 // 打印当前算子的名称
  std::string param = oper->param();  // 获取当前算子的参数
  if (!param.empty()) {
    os << "(" << param << ")";  // 如果参数不为空，打印参数
  }
  os << '\n';  // 换行

  if (static_cast<int>(ends.size()) < level + 2) {
    ends.resize(level + 2);  // 确保ends数组大小足够
  }
  ends[level + 1] = false;  // 该层级存在节点

  vector<std::unique_ptr<PhysicalOperator>> &children = oper->children();                   // 获取子操作符
  const auto                                 size     = static_cast<int>(children.size());  // 子操作符数量
  for (auto i = 0; i < size - 1; i++) {
    to_string(os, children[i].get(), level + 1, false /*last_child*/, ends);  // 递归打印非最后子节点
  }
  if (size > 0) {
    to_string(os, children[size - 1].get(), level + 1, true /*last_child*/, ends);  // 递归打印最后子节点
  }
}
