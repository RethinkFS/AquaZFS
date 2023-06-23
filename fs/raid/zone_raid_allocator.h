//
// Created by chiro on 23-6-3.
//

#ifndef ROCKSDB_ZONE_RAID_ALLOCATOR_H
#define ROCKSDB_ZONE_RAID_ALLOCATOR_H

#include <map>

#include "zone_raid.h"

// namespace std {
// template <typename T1, typename T2>
// struct less<std::pair<T1, T2>> {
//   bool operator()(const std::pair<T1, T2> &l,
//                   const std::pair<T1, T2> &r) const {
//     if (l.first == r.first) {
//       return l.second > r.second;
//     }
//
//     return l.first > r.first;
//   }
// };
// }  // namespace std

namespace aquafs {

class ZoneRaidAllocator {
 public:
  // use `map` or `unordered_map` to store raid mappings
  template <typename K, typename V>
  using map_use = std::map<K, V>;
  using device_zone_map_t = map_use<idx_t, std::vector<RaidMapItem>>;
  using mode_map_t = map_use<idx_t, RaidModeItem>;
  // <device idx, zone idx> -> raid zone idx, unsupported: unordered_map
  using device_zone_t = std::pair<idx_t, idx_t>;
  using device_zone_inv_map_t = map_use<device_zone_t, idx_t>;

  // map: raid zone idx (* sz) -> vec<device idx, device zone idx>
  device_zone_map_t device_zone_map_{};
  // map: <device idx, device zone idx> -> raid zone idx (* sz)
  device_zone_inv_map_t device_zone_inv_map_{};
  // map: raid zone idx -> raid mode, option
  mode_map_t mode_map_{};
  // offline zones
  map_use<device_zone_t, bool> offline_zones_;

  idx_t device_nr_{};
  idx_t zone_nr_{};

  const device_zone_map_t &getDeviceZoneMap() const { return device_zone_map_; }
  const mode_map_t &getModeMap() const { return mode_map_; }

  void setInfo(idx_t device_nr, idx_t zone_nr) {
    device_nr_ = device_nr;
    zone_nr_ = zone_nr;
  }

  Status addMapping(idx_t logical_raid_zone_sub_idx, idx_t physical_device_idx,
                    idx_t physical_zone_idx);
  void setMappingMode(idx_t logical_raid_zone_idx, RaidModeItem mode);
  void setMappingMode(idx_t logical_raid_zone_idx, RaidMode mode);

  int getFreeDeviceZone(idx_t device);
  int getFreeZoneDevice(idx_t device_zone);
  Status createMapping(idx_t logical_raid_zone_idx);
  Status createMappingTwice(idx_t logical_raid_zone_idx);
  Status createOneMappingAt(idx_t logical_raid_zone_sub_idx, idx_t device,
                            idx_t &zone);
  void setOffline(idx_t device, idx_t zone);
};

}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAID_ALLOCATOR_H
