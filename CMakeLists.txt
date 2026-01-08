cmake_minimum_required(VERSION 3.16)
project(xdbc-server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

#TODO: fix hardcoded paths
set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
set(SNAPPY_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libsnappy.so")

include_directories("/zfp/include")
link_directories(/zfp/lib)

find_package(ZLIB REQUIRED)
find_package(spdlog REQUIRED)
find_package(FastPFOR REQUIRED)

find_package(FPZIP REQUIRED)
find_library(FPZIP_LIBRARY NAMES fpzip)


#find_package(Arrow REQUIRED)
#find_package(Parquet REQUIRED)
#include_directories(${ARROW_INCLUDE_DIRS} ${PARQUET_INCLUDE_DIRS})

find_library(PARQUET_SHARED_LIB parquet REQUIRED)
# Ensure the library is found
if (NOT PARQUET_SHARED_LIB)
    message(FATAL_ERROR "Parquet shared library not found.")
endif()


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

# Common source files (excluding main.cpp)
set(XDBC_LIB_SOURCES
    xdbcserver.cpp
    xdbc_c_api.cpp
    Compression/Compressor.cpp
    DataSources/PGReader/PGReader.cpp
    DataSources/CHReader/CHReader.cpp
    DataSources/CSVReader/CSVReader.cpp
    DataSources/PQReader/PQReader.cpp
    DataSources/DataSource.cpp
)

# Build shared library
add_library(xdbc-server-lib SHARED ${XDBC_LIB_SOURCES})
target_link_libraries(xdbc-server-lib PUBLIC 
    clickhouse-cpp-lib 
    -lpqxx -lpq 
    ${USED_LIBS} 
    lzo2 lz4 cityhash 
    spdlog::spdlog 
    ZLIB::ZLIB 
    FastPFOR::FastPFOR
    ${FPZIP_LIBRARY} 
    ${ARROW_LIBRARIES} 
    -larrow 
    ${PARQUET_SHARED_LIB}
)

# Set library output name
set_target_properties(xdbc-server-lib PROPERTIES 
    OUTPUT_NAME xdbc-server
    VERSION 1.0.0
    SOVERSION 1
)

# Build executable
add_executable(xdbc-server main.cpp)
target_link_libraries(xdbc-server PRIVATE xdbc-server-lib)

# Install rules
install(TARGETS xdbc-server-lib xdbc-server
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES 
    xdbcserver.h 
    xdbc_c_api.h
    DataSources/DataSource.h
    customQueue.h
    DESTINATION include/xdbc-server
)