PROJECT (TileCaching CXX)
CMAKE_MINIMUM_REQUIRED (VERSION 3.0)
# ---[ C++11 Flags
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++1y" COMPILER_SUPPORTS_CXX1Y)

if(COMPILER_SUPPORTS_CXX1Y)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++1Y support. Please use a different C++ compiler.")
endif()

set(Peloton_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/lib/include)
set(Peloton_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/lib/third_party)
include_directories(${Peloton_SRC_INCLUDE_DIR} ${Peloton_THIRD_PARTY_INCLUDE_DIR} ${PROJECT_BINARY_DIR})

FILE(GLOB_RECURSE source ./lib/*.cpp)
FILE(GLOB_RECURSE headers ./lib/*.h)
add_library(tileanti SHARED ${source} ${headers})
add_executable(tabletest ${PROJECT_SOURCE_DIR}/test/storage/data_table_test.cpp)
target_link_libraries(tabletest tileanti -lpthread)
