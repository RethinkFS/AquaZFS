cmake_minimum_required(VERSION 3.4)

if (NOT DEFINED AQUAFS_EXPORT_PROMETHEUS)
    set(AQUAFS_EXPORT_PROMETHEUS OFF)
endif ()

set(AQUAFS_VERSION v0.0.1-alpha)

set(aquafs_SOURCES_local "fs/fs_aquafs.cc" "fs/zbd_aquafs.cc" "fs/io_aquafs.cc" "fs/zonefs_aquafs.cc"
        "fs/zbdlib_aquafs.cc"
        "fs/raid/zone_raid.cc" "fs/raid/zone_raid_auto.cc" "fs/raid/zone_raid0.cc" "fs/raid/zone_raid1.cc" "fs/raid/zone_raidc.cc"
        "fs/configuration.cc")
set(aquafs_HEADERS_local "fs/fs_aquafs.h" "fs/zbd_aquafs.h" "fs/io_aquafs.h" "fs/version.h" "fs/metrics.h"
        "fs/snapshot.h" "fs/filesystem_utility.h" "fs/zonefs_aquafs.h" "fs/zbdlib_aquafs.h"
        "fs/raid/zone_raid.h" "fs/raid/zone_raid_auto.h" "fs/raid/zone_raid0.h" "fs/raid/zone_raid1.h" "fs/raid/zone_raidc.h"
        "fs/configuration.h")
set(aquafs_LIBS_local "zbd")

project(aquafs)

file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fs/version.h "#pragma once\n#define AQUAFS_VERSION \"${AQUAFS_VERSION}\"\n")

set(aquafs_SOURCES ${aquafs_SOURCES_local})
set(aquafs_HEADERS ${aquafs_HEADERS_local})
set(aquafs_LIBS ${aquafs_LIBS_local} "gflags" "uring" "zstd" "lz4" "snappy" "bz2" "z" "numa")

#  file(GLOB_RECURSE port_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/port/*.cc)
#  file(GLOB_RECURSE util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/util/util/*.cc)
#  file(GLOB_RECURSE test_util_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_util/*.cc)

if (NOT AQUAFS_SKIP_LIBZBD_CHECK)
    find_package(PkgConfig REQUIRED)
    pkg_search_module(ZBD REQUIRED libzbd>=1.5.0)
    # pkg_search_module(rocksdb REQUIRED)
    find_package(gflags REQUIRED)
    add_definitions(-DGFLAGS=1)
    include_directories(${GFLAGS_INCLUDE_DIR})
    # find_package(NUMA REQUIRED)
    add_definitions(-DNUMA)
    # include_directories(${NUMA_INCLUDE_DIR})
endif ()

#  add_library(port STATIC ${port_SOURCES})
#  add_library(util STATIC ${util_SOURCES})

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/util)
add_library(aaquafs STATIC ${aquafs_SOURCES} ${aquafs_HEADERS})
target_link_libraries(aaquafs ${aquafs_LIBS})

add_definitions(-DWITH_TERARKDB=1 -DOS_LINUX=1 -DROCKSDB_PLATFORM_POSIX=1)
add_executable(aquafs ${CMAKE_CURRENT_SOURCE_DIR}/util/aquafs.cc)
target_link_libraries(aquafs aaquafs)

add_executable(defconfig ${CMAKE_CURRENT_SOURCE_DIR}/util/defconfig.cc)
target_link_libraries(defconfig aaquafs)

# basic test
enable_testing()
add_test(NAME aquafs-mkfs COMMAND sudo $<TARGET_FILE:aquafs> mkfs --zbd=nullb0 --aux_path=/tmp/aux_path --force)
add_test(NAME aquafs-list COMMAND sudo $<TARGET_FILE:aquafs> list --zbd=nullb0)

add_test(NAME aquafs-mkfs-raid0 COMMAND sudo $<TARGET_FILE:aquafs> mkfs --raids=raid0:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)
add_test(NAME aquafs-mkfs-raid1 COMMAND sudo $<TARGET_FILE:aquafs> mkfs --raids=raid1:dev:nullb0,dev:nullb1 --aux_path=/tmp/aux_path --force)
