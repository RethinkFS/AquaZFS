//
// Created by chiro on 23-6-5.
//

#include "aquafs_utils.h"

DEFINE_bool(delay_sim, true, "Add simulated delay to read/write");
DEFINE_uint64(delay_us_transmit, 30,
              "Simulated delay time for transmitting data");
DEFINE_uint64(delay_us_data, 5,
              "Simulated delay time for reading/writing per data block");
DEFINE_uint64(delay_data_blksz, 512,
              "Simulated block size for reading/writing delay");

namespace aquafs {

uint32_t calculate_delay_us(uint64_t size) {
  return static_cast<uint32_t>(((size / FLAGS_delay_data_blksz) + 1) *
                                   FLAGS_delay_us_data +
                               FLAGS_delay_us_transmit);
}

}  // namespace aquafs