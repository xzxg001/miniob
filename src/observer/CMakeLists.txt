#打印当前处理的源代码目录路径
MESSAGE(STATUS "This is CMAKE_CURRENT_SOURCE_DIR dir " ${CMAKE_CURRENT_SOURCE_DIR})

# 将当前源代码目录添加到头文件搜索路径中
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

# 递归地查找当前目录下所有的.cpp和.c文件，并将它们存储在ALL_SRC变量中
FILE(GLOB_RECURSE ALL_SRC *.cpp *.c)

# 设置主源文件的路径
SET(MAIN_SRC ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
# 打印主源文件的路径
MESSAGE("MAIN SRC: " ${MAIN_SRC})
# 遍历所有源文件
FOREACH (F ${ALL_SRC})
    # 如果当前文件不是主源文件，则将其添加到LIB_SRC变量中，并打印使用该文件
    IF (NOT ${F} STREQUAL ${MAIN_SRC})
        SET(LIB_SRC ${LIB_SRC} ${F})
        MESSAGE("Use " ${F})
    ENDIF()

ENDFOREACH (F)

# 设置静态链接Libevent库
SET(LIBEVENT_STATIC_LINK TRUE)

# 查找Libevent库，这是一个事件处理库，如果找不到则报错
FIND_PACKAGE(Libevent CONFIG REQUIRED)


# JsonCpp库不能使用FIND_PACKAGE正常工作，可能需要手动下载或使用其他方法集成

# 设置要链接的库
SET(LIBRARIES common pthread dl libevent::core libevent::pthreads libjsoncpp.a)

# 指定目标文件位置
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)# 设置可执行文件的输出路径
MESSAGE("Binary directory:" ${EXECUTABLE_OUTPUT_PATH})# 打印可执行文件的输出路径
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)# 设置库文件的输出路径
MESSAGE("Archive directory:" ${LIBRARY_OUTPUT_PATH})# 打印库文件的输出路径

# 添加一个名为observer的可执行目标，它依赖于主源文件
ADD_EXECUTABLE(observer ${MAIN_SRC})
# 为目标observer链接静态库observer_static
TARGET_LINK_LIBRARIES(observer observer_static)

# 添加一个名为observer_static的静态库目标，它依赖于LIB_SRC中的源文件
ADD_LIBRARY(observer_static STATIC ${LIB_SRC})


INCLUDE (readline)# 包含readline库的CMake模块
MINIOB_FIND_READLINE()# 自定义的查找Readline库的宏
IF (HAVE_READLINE)# 如果找到了Readline库
    TARGET_LINK_LIBRARIES(observer_static ${READLINE_LIBRARY})# 为目标observer_static链接Readline库
    TARGET_INCLUDE_DIRECTORIES(observer_static PRIVATE ${READLINE_INCLUDE_DIR})# 为目标observer_static添加头文件搜索路径
    ADD_DEFINITIONS(-DUSE_READLINE)# 定义预处理器宏USE_READLINE
    MESSAGE ("observer_static use readline")# 打印Readline库已被使用的消息
ELSE ()
    MESSAGE ("readline is not found")# 如果没有找到Readline库，打印未找到的消息
ENDIF()

# 设置observer_static目标的输出名称为observer
SET_TARGET_PROPERTIES(observer_static PROPERTIES OUTPUT_NAME observer)
# 为目标observer_static链接之前定义的LIBRARIES中的库
TARGET_LINK_LIBRARIES(observer_static ${LIBRARIES})

# Target 必须在定义 ADD_EXECUTABLE 之后， programs 不受这个限制
# TARGETS和PROGRAMS 的默认权限是OWNER_EXECUTE, GROUP_EXECUTE, 和WORLD_EXECUTE，即755权限， programs 都是处理脚本类
# 类型分为RUNTIME／LIBRARY／ARCHIVE, prog

#安装observer可执行文件和observer_static库文件
# 可执行文件安装到bin目录，库文件安装到lib目录
INSTALL(TARGETS observer observer_static 
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib)
