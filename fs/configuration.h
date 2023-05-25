//
// Created by chiro on 23-5-9.
//

#ifndef ROCKSDB_CONFIGURATION_H
#define ROCKSDB_CONFIGURATION_H

#include <gflags/gflags.h>

DECLARE_uint64(gc_start_level);
DECLARE_uint64(gc_slope);
DECLARE_uint64(gc_sleep_time);

#endif  // ROCKSDB_CONFIGURATION_H
