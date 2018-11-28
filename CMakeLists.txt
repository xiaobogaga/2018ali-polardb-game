# 声明要求的cmake最低版本
cmake_minimum_required( VERSION 2.8 )

# 添加c++11标准支持
set( CMAKE_CXX_FLAGS "-std=c++11" )

# 声明一个cmake工程
project( engine_race )


# 找到后面需要库和头文件的包
# find_package（包的名称及最低版本）
# 例如find_package(OpenCV 2.4.3 REQUIRED)


# 头文件
include_directories(
        include
)
# 例如
#include_directories(
# ${PROJECT_SOURCE_DIR}
# ${PROJECT_SOURCE_DIR}/include
# ${EIGEN3_INCLUDE_DIR}
# )

# 设置路径（下面生成共享库的路径）
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)
# 即生成的共享库在工程文件夹下的lib文件夹中

# 创建共享库（把工程内的cpp文件都创建成共享库文件，方便通过头文件来调用）
add_library(${PROJECT_NAME} SHARED
        engine_race/engine_race.cc
        engine_race/basic.cc
        engine_race/bplustree.cc
        engine_race/data_store.cc
        engine_race/door_plate.cc
        engine_race/util.cc
        engine_race/bitvector.cc
        engine_race/hash.cc
        # engine_race/testBplusTree.cpp
        engine_race/indexstore.cc
        # engine_race/testRadixTree.cpp
        # engine_race/test.cc
        # engine_race/test_with_kill.cc
        )
# 这时候只需要cpp，不需要有主函数
# ${PROJECT_NAME}是生成的库名 表示生成的共享库文件就叫做 lib工程名.so
# 也可以专门写cmakelists来编译一个没有主函数的程序来生成共享库，供其它程序使用

# 链接库
# 把刚刚生成的${PROJECT_NAME}库和所需的其它库链接起来
#target_link_libraries(${PROJECT_NAME}
#/usr/lib/i386-linux-gnu/libboost_system.so
#)

# 编译主函数，生成可执行文件

# 先设置路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

# 可执行文件生成
add_executable(test1 engine_race/testBplusTree.cpp)

# 这个可执行文件所需的库（一般就是刚刚生成的工程的库咯）
target_link_libraries(test1 ${PROJECT_NAME})

# 可执行文件生成
add_executable(test2 engine_race/test.cc)

# 这个可执行文件所需的库（一般就是刚刚生成的工程的库咯）
target_link_libraries(test2 ${PROJECT_NAME})

add_executable(test3 engine_race/test_with_kill.cc)

target_link_libraries(test3
        ${PROJECT_NAME}
        pthread
        )

add_executable(tempTest engine_race/tempTest.cpp)

target_link_libraries(tempTest ${PROJECT_NAME})

add_executable(radixTreeTest engine_race/testRadixTree.cpp)

target_link_libraries(radixTreeTest ${PROJECT_NAME})