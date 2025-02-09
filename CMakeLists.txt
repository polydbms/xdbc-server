cmake_minimum_required(VERSION 3.16)
project(xdbc-server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g -std=gnu++17")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
#[[SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pg")]]

#TODO: fix hardcoded paths
set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
set(SNAPPY_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libsnappy.so")


set(LIBRARY_SOURCES
    xdbcserver.cpp
    Compression/Compressor.cpp
    DataSources/PGReader/PGReader.cpp
    DataSources/PGInternalReader/PGInternalReader.cpp
    DataSources/CHReader/CHReader.cpp
    DataSources/CSVReader/CSVReader.cpp
    DataSources/DataSource.cpp
)

include_directories("/zfp/include")
link_directories(/zfp/lib)

find_package(ZLIB REQUIRED)
find_package(spdlog REQUIRED)
find_package(FastPFOR REQUIRED)
find_package(FPZIP REQUIRED)
find_library(FPZIP_LIBRARY NAMES fpzip)

find_path(CITYHASH_INCLUDE_DIR NAMES city.h)
find_library(CITYHASH_LIBRARIES NAMES cityhash)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
        CITYHASH DEFAULT_MSG
        CITYHASH_LIBRARIES CITYHASH_INCLUDE_DIR)

if (NOT CITYHASH_FOUND)
    message(STATUS "Using third-party bundled CityHash")
else ()
    message(STATUS "Found CITYHASH: ${CITYHASH_LIBRARIES}")
endif (NOT CITYHASH_FOUND)

mark_as_advanced(CITYHASH_INCLUDE_DIR CITYHASH_LIBRARIES)

find_package(Boost 1.40.0 REQUIRED system program_options)

IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
    LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
ENDIF (Boost_FOUND)
SET(USED_LIBS ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_REGEX_LIBRARY} ${ZSTD_LIBRARY_PATH} ${SNAPPY_LIBRARY_PATH})

#add_definitions(-DUSE_SYSTEM_ABSEIL=OFF)
find_package(FPZIP REQUIRED)
find_package(fmt 8.1.1 REQUIRED)
find_package(FastPFOR REQUIRED)
find_package(Snappy REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(PostgreSQL REQUIRED)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${PostgreSQL_INCLUDE_DIRS}
    /usr/include/postgresql/14/server
)

add_library(xdbcserver_lib SHARED ${LIBRARY_SOURCES})
execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE PG_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS "PostgreSQL Library Directory: ${PG_LIB_DIR}")
find_library(POSTGRES_SERVER_LIB
    NAMES pq
    PATHS ${PG_LIB_DIR}
    NO_DEFAULT_PATH
    REQUIRED
)
target_link_libraries(xdbcserver_lib
    ${PostgreSQL_LIBRARIES}
    ${POSTGRES_SERVER_LIB}
    clickhouse-cpp-lib
    boost_program_options
    boost_system
    lzo2
    lz4
    cityhash
    pqxx
    fpzip
    ${FPZIP_LIBRARIES}
    fmt::fmt
    spdlog::spdlog
    ZLIB::ZLIB
    FastPFOR::FastPFOR
    Snappy::snappy
)

# note : idk how to compile the executable with PGIntaernalReader. for the server probably PGInternal will never be relevant. so maybe instead of fixing this just remove PGInternalReader from the executable
# Create executable
#add_executable(${PROJECT_NAME} main.cpp xdbcserver.cpp Compression/Compressor.cpp  DataSources/PGInternalReader/PGInternalReader.cpp DataSources/PGReader/PGReader.cpp DataSources/CHReader/CHReader.cpp DataSources/CSVReader/CSVReader.cpp DataSources/DataSource.cpp)
# Link executable
#target_link_libraries(${PROJECT_NAME}
#    PRIVATE
#    clickhouse-cpp-lib
#    ${USED_LIBS}
#    lzo2
#    lz4
#    cityhash
#    spdlog::spdlog
#    ZLIB::ZLIB
#    FastPFOR::FastPFOR
#    ${FPZIP_LIBRARY}
#    ${PostgreSQL_LIBRARIES} 
#)
