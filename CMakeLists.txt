cmake_minimum_required(VERSION 3.16)
project(xdbc-server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -pthread)

#TODO: fix hardcoded paths
set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
set(SNAPPY_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libsnappy.so")

include_directories("/zfp/include")
link_directories(/zfp/lib)

find_package(ZLIB REQUIRED)
find_package(spdlog REQUIRED)
find_package(Boost 1.40.0 REQUIRED system program_options)
IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
    LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
ENDIF (Boost_FOUND)
SET(USED_LIBS ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_REGEX_LIBRARY} ${ZSTD_LIBRARY_PATH} ${SNAPPY_LIBRARY_PATH})

#add_definitions(-DUSE_SYSTEM_ABSEIL=OFF)
#add_executable(${PROJECT_NAME} main.cpp pg.cpp ch.cpp pgserver.cpp compresslibs.cpp compresslibs.h)
add_executable(${PROJECT_NAME} main.cpp pg.cpp ch.cpp xdbcserver.cpp Compression/Compressor.cpp DataSources/PGReader/PGReader.cpp)
#add_library(clickhouse-cpp SHARED IMPORTED)
#set_property(TARGET clickhouse-cpp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libclickhouse-cpp-lib.so")
target_link_libraries(${PROJECT_NAME} PRIVATE clickhouse-cpp-lib -lpqxx -lpq ${USED_LIBS} lzo2 lz4 cityhash zfp spdlog::spdlog ZLIB::ZLIB)