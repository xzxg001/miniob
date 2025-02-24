/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_YACC_SQL_HPP_INCLUDED  // 检查是否已经包含了该头文件
# define YY_YY_YACC_SQL_HPP_INCLUDED  // 定义头文件保护宏

/* Debug traces.  */
#ifndef YYDEBUG  // 如果没有定义YYDEBUG
# define YYDEBUG 0  // 设置YYDEBUG为0，表示关闭调试信息
#endif
#if YYDEBUG  // 如果YYDEBUG为真
extern int yydebug;  // 外部声明调试变量
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE  // 如果没有定义YYTOKENTYPE
# define YYTOKENTYPE  // 开始定义YYTOKENTYPE
  enum yytokentype  // 定义一个枚举类型，列出所有词法单元
  {
    YYEMPTY = -2,  // 特殊值，表示空
    YYEOF = 0,     // "end of file" 的标记
    YYerror = 256, // error 的标记
    YYUNDEF = 257, // "invalid token" 的标记
    SEMICOLON = 258,  // SEMICOLON 词法单元
    BY = 259,        // BY 词法单元
    CREATE = 260,    // CREATE 词法单元
    DROP = 261,      // DROP 词法单元
    GROUP = 262,     // GROUP 词法单元
    TABLE = 263,     // TABLE 词法单元
    TABLES = 264,    // TABLES 词法单元
    INDEX = 265,     // INDEX 词法单元
    CALC = 266,      // CALC 词法单元
    SELECT = 267,    // SELECT 词法单元
    DESC = 268,      // DESC 词法单元
    SHOW = 269,      // SHOW 词法单元
    SYNC = 270,      // SYNC 词法单元
    INSERT = 271,    // INSERT 词法单元
    DELETE = 272,    // DELETE 词法单元
    UPDATE = 273,    // UPDATE 词法单元
    LBRACE = 274,    // LBRACE 词法单元
    RBRACE = 275,    // RBRACE 词法单元
    COMMA = 276,     // COMMA 词法单元
    TRX_BEGIN = 277, // TRX_BEGIN 词法单元
    TRX_COMMIT = 278,// TRX_COMMIT 词法单元
    TRX_ROLLBACK = 279, // TRX_ROLLBACK 词法单元
    INT_T = 280,     // INT_T 词法单元
    STRING_T = 281,  // STRING_T 词法单元
    FLOAT_T = 282,   // FLOAT_T 词法单元
    VECTOR_T = 283,  // VECTOR_T 词法单元
    HELP = 284,      // HELP 词法单元
    EXIT = 285,      // EXIT 词法单元
    DOT = 286,       // DOT 词法单元
    INTO = 287,      // INTO 词法单元
    VALUES = 288,    // VALUES 词法单元
    FROM = 289,      // FROM 词法单元
    WHERE = 290,     // WHERE 词法单元
    AND = 291,       // AND 词法单元
    SET = 292,       // SET 词法单元
    ON = 293,        // ON 词法单元
    LOAD = 294,      // LOAD 词法单元
    DATA = 295,      // DATA 词法单元
    INFILE = 296,    // INFILE 词法单元
    EXPLAIN = 297,   // EXPLAIN 词法单元
    STORAGE = 298,   // STORAGE 词法单元
    FORMAT = 299,    // FORMAT 词法单元
    EQ = 300,        // EQ 词法单元
    LT = 301,        // LT 词法单元
    GT = 302,        // GT 词法单元
    LE = 303,        // LE 词法单元
    GE = 304,        // GE 词法单元
    NE = 305,        // NE 词法单元
    NUMBER = 306,    // NUMBER 词法单元
    FLOAT = 307,     // FLOAT 词法单元
    ID = 308,        // ID 词法单元
    SSS = 309,       // SSS 词法单元
    UMINUS = 310     // UMINUS 词法单元
  };
  typedef enum yytokentype yytoken_kind_t;  // 为枚举类型定义一个别名
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED  // 如果没有定义YYSTYPE
union YYSTYPE  // 定义一个联合体，用于存储各种类型的词法值
{
  ParsedSqlNode * sql_node;                      // 解析后的SQL节点
  ConditionSqlNode * condition;                  // 条件SQL节点
  Value * value;                                  // 值
  enum CompOp comp;                              // 比较操作符
  RelAttrSqlNode * rel_attr;                     // 关系属性SQL节点
  std::vector<AttrInfoSqlNode> * attr_infos;      // 属性信息SQL节点列表
  AttrInfoSqlNode * attr_info;                   // 属性信息SQL节点
  Expression * expression;                       // 表达式
  std::vector<std::unique_ptr<Expression>> * expression_list; // 表达式列表
  std::vector<Value> * value_list;               // 值列表
  std::vector<ConditionSqlNode> * condition_list; // 条件列表
  std::vector<RelAttrSqlNode> * rel_attr_list;    // 关系属性列表
  std::vector<std::string> * relation_list;       // 关系列表
  char * string;                                  // 字符串
  int number;                                     // 整数
  float floats;                                   // 浮点数
};

typedef union YYSTYPE YYSTYPE;  // 为联合体定义一个别名
# define YYSTYPE_IS_TRIVIAL 1  // 定义YYSTYPE的存储类别
# define YYSTYPE_IS_DECLARED 1  // 声明YYSTYPE已经被定义
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED  // 如果没有定义YYLTYPE
typedef struct YYLTYPE YYLTYPE;  // 定义一个结构体，用于存储词法单元的位置信息
struct YYLTYPE
{
  int first_line;  // 词法单元的开始行号
  int first_column;  // 词法单元的开始列号
  int last_line;  // 词法单元的结束行号
  int last_column;  // 词法单元的结束列号
};

# define YYLTYPE_IS_DECLARED 1  // 声明YYLTYPE已经被定义
# define YYLTYPE_IS_TRIVIAL 1  // 定义YYLTYPE的存储类别
#endif

// 声明解析函数，该函数将使用上述定义的数据结构
int yyparse (const char * sql_string, ParsedSqlResult * sql_result, void * scanner);

#endif /* !YY_YY_YACC_SQL_HPP_INCLUDED  */  // 结束头文件保护宏
