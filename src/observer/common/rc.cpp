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
// Created by Wangyunlai on 2021/5/14.
//

#include "common/rc.h"

/*
 * @brief 将返回码转换为字符串表示
 * @param rc 返回码，类型为 RC
 * @return 对应的字符串表示
 */
const char *strrc(RC rc)
{
#define DEFINE_RC(name)                                \
  case RC::name: {                                     \
    return #name; /* 返回对应的字符串名称 */ \
  } break;

  /* 根据不同的返回码，返回相应的字符串 */
  switch (rc) {
    DEFINE_RCS; /* 使用宏定义的所有返回码 */
    default: {
      return "unknown"; /* 如果不在已定义的返回码中，返回“unknown” */
    }
  }
#undef DEFINE_RC
}

/*
 * @brief 检查返回码是否表示成功
 * @param rc 返回码，类型为 RC
 * @return 如果返回码是 SUCCESS，返回 true；否则返回 false
 */
bool OB_SUCC(RC rc) { return rc == RC::SUCCESS; }

/*
 * @brief 检查返回码是否表示失败
 * @param rc 返回码，类型为 RC
 * @return 如果返回码不是 SUCCESS，返回 true；否则返回 false
 */
bool OB_FAIL(RC rc) { return rc != RC::SUCCESS; }
