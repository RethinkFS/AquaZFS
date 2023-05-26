cmake_minimum_required(VERSION 3.20)

if (NOT DEFINED AQUAFS_VERSION)
    set(AQUAFS_VERSION v0.0.1-SNAPSHOT)
endif ()

project(aquazfs)

file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fs/version.h "#pragma once\n#define AQUAFS_VERSION \"${AQUAFS_VERSION}\"\n")

set(aquafs_LIBS "zbd" "gflags" "uring" "zstd" "lz4" "snappy" "bz2" "z" "numa")

file(GLOB_RECURSE aquafs_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/**/*.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/**/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/**/*.c)

file(GLOB_RECURSE aquafs_HEADERS
        ${CMAKE_CURRENT_SOURCE_DIR}/**/*.h)

find_package(PkgConfig REQUIRED)
pkg_search_module(ZBD REQUIRED libzbd>=1.5.0)
# pkg_search_module(rocksdb REQUIRED)
find_package(gflags REQUIRED)
add_definitions(-DGFLAGS=1)
include_directories(${GFLAGS_INCLUDE_DIR})

add_definitions(-DWITH_TERARKDB=1 -DOS_LINUX=1 -DROCKSDB_PLATFORM_POSIX=1)

# find_package(NUMA REQUIRED)
#add_definitions(-DNUMA)
# include_directories(${NUMA_INCLUDE_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/util ${CMAKE_CURRENT_SOURCE_DIR}/base)
add_library(aquaz STATIC ${aquafs_SOURCES} ${aquafs_HEADERS})
target_link_libraries(aquaz ${aquafs_LIBS})

file(GLOB programs ${CMAKE_CURRENT_SOURCE_DIR}/programs/*.cc)
foreach (program ${programs})
    cmake_path(GET program STEM name)
    add_executable(${name} ${program})
    target_link_libraries(${name} aquaz)
endforeach ()

# basic test
enable_testing()
add_test(NAME aquafs-mkfs COMMAND sudo $<TARGET_FILE:aquazfs> mkfs --zbd=nullb0 --aux_path=/tmp/aux_path --force)
add_test(NAME aquafs-list COMMAND sudo $<TARGET_FILE:aquazfs> list --zbd=nullb0)

add_test(NAME aquafs-mkfs-raid0 COMMAND sudo $<TARGET_FILE:aquazfs> mkfs --raids=raid0:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)
add_test(NAME aquafs-mkfs-raid1 COMMAND sudo $<TARGET_FILE:aquazfs> mkfs --raids=raid1:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)
