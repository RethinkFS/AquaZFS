//
// Created by chiro on 23-6-5.
//

#ifndef ROCKSDB_AQUAFS_UTILS_H
#define ROCKSDB_AQUAFS_UTILS_H

#include <gflags/gflags.h>
#include <sys/select.h>

#include <cstdint>
#include <ctime>

DECLARE_bool(delay_sim);
DECLARE_uint64(delay_us_transmit);
DECLARE_uint64(delay_us_data);
DECLARE_uint64(delay_data_blksz);

namespace aquafs {

static inline void delay_ms(uint32_t ms) {
  struct timespec ts {};
  ts.tv_sec = ms / 1000;
  ts.tv_nsec = (ms % 1000) * 1000000;
  nanosleep(&ts, nullptr);
}

static inline void delay_us(uint32_t us) {
  // use select
  struct timeval tv {};
  tv.tv_sec = us / 1000000;
  tv.tv_usec = us % 1000000;
  select(0, nullptr, nullptr, nullptr, &tv);
}

uint32_t calculate_delay_us(uint64_t size);

}  // namespace aquafs

#endif  // ROCKSDB_AQUAFS_UTILS_H
