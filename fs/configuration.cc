//
// Created by chiro on 23-5-9.
//

#include "configuration.h"

DEFINE_uint64(gc_start_level, 20, "Enable GC when percent < n%");
DEFINE_uint64(gc_slope, 3, "GC aggressiveness");
DEFINE_uint64(gc_sleep_time, 10 * 1000, "GC sleep time between running capacity detection");