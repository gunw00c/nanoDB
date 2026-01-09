cmake_minimum_required(VERSION 3.16)
project(nanodb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(NANODB_SOURCES
    src/storage/disk_manager.cpp
    src/catalog/catalog.cpp
    src/parser/sql_parser.cpp
    src/nanodb.cpp
)

# Create library
add_library(nanodb_lib ${NANODB_SOURCES})
target_include_directories(nanodb_lib PUBLIC ${CMAKE_SOURCE_DIR}/include)

# Create executable
add_executable(nanodb main.cpp)
target_link_libraries(nanodb nanodb_lib)
