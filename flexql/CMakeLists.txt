cmake_minimum_required(VERSION 3.16)
project(flexql LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${CMAKE_SOURCE_DIR}/include)

set(FLEXQL_COMMON_SOURCES
    src/network/socket_utils.cpp
    src/parser/sql_parser.cpp
    src/storage/database_engine.cpp
    src/storage/wal_writer.cpp
)

add_library(flexql_client_api STATIC
    src/client/flexql_api.cpp
    ${FLEXQL_COMMON_SOURCES}
)

target_compile_definitions(flexql_client_api PRIVATE FLEXQL_CLIENT_BUILD)

add_executable(flexql-server
    src/server/server_main.cpp
    src/server/server.cpp
    ${FLEXQL_COMMON_SOURCES}
)

add_executable(flexql-client
    src/client/client_main.cpp
)

target_link_libraries(flexql-client PRIVATE flexql_client_api)

if(WIN32)
    target_link_libraries(flexql_client_api PRIVATE ws2_32)
    target_link_libraries(flexql-server PRIVATE ws2_32)
endif()

add_executable(flexql-smoke tests/smoke.cpp)
target_link_libraries(flexql-smoke PRIVATE flexql_client_api)

add_executable(flexql-benchmark ../benchmark_flexql.cpp)
target_include_directories(flexql-benchmark PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(flexql-benchmark PRIVATE flexql_client_api)
if(WIN32)
    target_link_libraries(flexql-benchmark PRIVATE ws2_32)
endif()
