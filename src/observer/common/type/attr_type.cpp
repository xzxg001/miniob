/*
版权 (c) 2021 OceanBase 及其附属机构。保留所有权利。
miniob 依据 Mulan PSL v2 许可。
您可以根据 Mulan PSL v2 的条款和条件使用本软件。
您可以在以下网址获取 Mulan PSL v2 的副本：
         http://license.coscl.org.cn/MulanPSL2
本软件是按“原样”提供的，不提供任何类型的保证，
无论是明示还是暗示，包括但不限于不侵权、
适销性或特定用途的适用性。
有关详细信息，请参阅 Mulan PSL v2。
*/

#include "common/lang/string.h"     // 引入字符串处理相关的头文件
#include "common/type/attr_type.h"  // 引入属性类型相关的头文件

// 定义属性类型名称数组，索引与 AttrType 枚举值对应
const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "booleans"};

/**
 * @brief 将属性类型转换为字符串表示
 *
 * @param type 要转换的属性类型，类型为 AttrType。
 * @return 返回对应的字符串表示。如果类型无效，返回 "unknown"。
 */
const char *attr_type_to_string(AttrType type)
{
  // 检查传入的属性类型是否在有效范围内
  if (type >= AttrType::UNDEFINED && type < AttrType::MAXTYPE) {
    // 返回对应的字符串表示
    return ATTR_TYPE_NAME[static_cast<int>(type)];
  }
  // 如果类型无效，返回 "unknown"
  return "unknown";
}

/**
 * @brief 将字符串转换为属性类型
 *
 * @param s 要转换的字符串，代表属性类型的名称。
 * @return 返回对应的枚举类型 AttrType。如果没有找到匹配项，则返回 AttrType::UNDEFINED。
 */
AttrType attr_type_from_string(const char *s)
{
  // 遍历属性类型名称数组
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    // 忽略大小写比较字符串
    if (0 == strcasecmp(ATTR_TYPE_NAME[i], s)) {
      // 返回匹配的属性类型
      return (AttrType)i;
    }
  }
  // 如果没有找到匹配，返回未定义的属性类型
  return AttrType::UNDEFINED;
}
