/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/type/char_type.h"     // 引入 CharType 类的头文件
#include "common/type/float_type.h"    // 引入 FloatType 类的头文件
#include "common/type/integer_type.h"  // 引入 IntegerType 类的头文件
#include "common/type/data_type.h"     // 引入 DataType 类的头文件

// 静态成员变量初始化：存储所有数据类型实例的数组
array<unique_ptr<DataType>, static_cast<int>(AttrType::MAXTYPE)> DataType::type_instances_ = {
    make_unique<DataType>(AttrType::UNDEFINED),  // 未定义类型
    make_unique<CharType>(),                     // 字符类型
    make_unique<IntegerType>(),                  // 整型类型
    make_unique<FloatType>(),                    // 浮点类型
    make_unique<DataType>(AttrType::BOOLEANS),   // 布尔类型
};
