cmake_minimum_required(VERSION 3.16)
project(xdbc-server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -pthread)

set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
find_package(Boost 1.40.0 REQUIRED system)


IF(Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
    LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
ENDIF(Boost_FOUND)
SET(USED_LIBS ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_REGEX_LIBRARY})

#add_definitions(-DUSE_SYSTEM_ABSEIL=OFF)
add_executable(${PROJECT_NAME} main.cpp pg.cpp ch.cpp pgserver.cpp)
#add_library(clickhouse-cpp SHARED IMPORTED)
#set_property(TARGET clickhouse-cpp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libclickhouse-cpp-lib.so")
target_link_libraries(${PROJECT_NAME} ${ZSTD_LIBRARY_PATH} clickhouse-cpp-lib -lpqxx -lpq ${USED_LIBS})