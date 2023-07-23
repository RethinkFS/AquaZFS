//
// Created by lyt on 7/3/23.
//

#ifndef ROCKSDB_ZONE_RAID5_H
#define ROCKSDB_ZONE_RAID5_H

#include "zone_raid.h"
#include "zone_raid_allocator.h"

namespace aquafs {
class Raid5ZoneBlockDevice : public AbstractRaidZonedBlockDevice {
 public:
  // template <typename K, typename V>
  // using map_use = ZoneRaidAllocator::map_use<K, V>;
  using device_zone_map_t = ZoneRaidAllocator::device_zone_map_t;
  using mode_map_t = ZoneRaidAllocator::mode_map_t;
  using raid_zone_t = struct zbd_zone;

  ZoneRaidAllocator allocator;
  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override;
  Raid5ZoneBlockDevice(
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
  template <typename T>
  auto get_idx_block(T pos) const {
    return pos / static_cast<T>(GetBlockSize());
  }
  template <typename T>
  auto get_idx_dev(T pos) const {
    return get_idx_dev(pos, nr_dev_t<T>() - 1);
  }
  template <typename T>
  auto get_idx_dev(T pos, T m) const {
    return get_idx_block(pos) % m;
  }
  template <typename T>
  auto req_pos(T pos) const {
    auto blk_offset = pos % static_cast<T>(GetBlockSize());
    return blk_offset + ((pos - blk_offset) / GetBlockSize()) / (nr_dev() - 1) *
                            GetBlockSize();
  }
  template <class T>
  RaidMapItem getAutoDeviceZone(T pos);
  template <class T>
  idx_t getAutoDeviceZoneIdx(T pos);
  template <class T>
  T getAutoMappedDevicePos(T pos);
  int raid5_check(int zone_start_index, int zone_end_index, bool direct);
  int raid5_update(int zone_start_index, int zone_end_index, bool direct);
  int raid5_restore(int device_id, int zone_index, char *restore_data,
                    bool direct);
};
}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAID5_H
