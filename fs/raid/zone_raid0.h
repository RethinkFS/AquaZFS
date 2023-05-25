//
// Created by chiro on 23-5-6.
//

#ifndef ROCKSDB_ZONE_RAID0_H
#define ROCKSDB_ZONE_RAID0_H

#include <cstdint>

#include "zone_raid.h"

namespace aquafs {
class Raid0ZonedBlockDevice : public AbstractRaidZonedBlockDevice {
 protected:
  template <typename T>
  auto get_idx_block(T pos) const {
    return pos / static_cast<T>(GetBlockSize());
  }
  template <typename T>
  auto get_idx_dev(T pos) const {
    return get_idx_dev(pos, nr_dev_t<T>());
  }
  template <typename T>
  auto get_idx_dev(T pos, T m) const {
    return get_idx_block(pos) % m;
  }
  template <typename T>
  auto req_pos(T pos) const {
    auto blk_offset = pos % static_cast<T>(GetBlockSize());
    return blk_offset +
           ((pos - blk_offset) / GetBlockSize()) / nr_dev() * GetBlockSize();
  }

 public:
  Raid0ZonedBlockDevice(
      const std::shared_ptr<Logger> &logger,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices);

  std::unique_ptr<ZoneList> ListZones() override;
  IOStatus Reset(uint64_t start, bool *offline,
                 uint64_t *max_capacity) override;
  IOStatus Finish(uint64_t start) override;
  IOStatus Close(uint64_t start) override;
  int Read(char *buf, int size, uint64_t pos, bool direct) override;
  int Write(char *data, uint32_t size, uint64_t pos) override;
  int InvalidateCache(uint64_t pos, uint64_t size) override;
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                      unsigned int idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                    unsigned int idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           unsigned int idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;

 protected:
  void syncBackendInfo() override;
};
}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAID0_H
