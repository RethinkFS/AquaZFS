//
// Created by chiro on 23-5-6.
//

#ifndef ROCKSDB_ZONE_RAID_AUTO_H
#define ROCKSDB_ZONE_RAID_AUTO_H

#include "zone_raid.h"

namespace aquafs {
class RaidAutoZonedBlockDevice : public AbstractRaidZonedBlockDevice {
 public:
  // use `map` or `unordered_map` to store raid mappings
  template <typename K, typename V>
  using map_use = std::unordered_map<K, V>;
  using device_zone_map_t = map_use<idx_t, RaidMapItem>;
  using mode_map_t = map_use<idx_t, RaidModeItem>;
  using raid_zone_t = struct zbd_zone;

 private:
  // map: raid zone idx (* sz) -> device idx, device zone idx
  device_zone_map_t device_zone_map_{};
  // map: raid zone idx -> raid mode, option
  mode_map_t mode_map_{};
  // auto-raid: manually managed zone info
  std::unique_ptr<raid_zone_t> a_zones_{};

  void flush_zone_info();

  void syncBackendInfo() override;

 public:
  explicit RaidAutoZonedBlockDevice(
      const std::shared_ptr<Logger> &logger,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices);

  void layout_update(device_zone_map_t &&device_zone, mode_map_t &&mode_map);
  void layout_setup(device_zone_map_t &&device_zone, mode_map_t &&mode_map);
  const device_zone_map_t &getDeviceZoneMap() const { return device_zone_map_; }
  const mode_map_t &getModeMap() const { return mode_map_; }

  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override;
  std::unique_ptr<ZoneList> ListZones() override;
  IOStatus Reset(uint64_t start, bool *offline,
                 uint64_t *max_capacity) override;
  IOStatus Finish(uint64_t start) override;
  IOStatus Close(uint64_t start) override;
  int Read(char *buf, int size, uint64_t pos, bool direct) override;
  int Write(char *data, uint32_t size, uint64_t pos) override;
  int InvalidateCache(uint64_t pos, uint64_t size) override;
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, idx_t idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           idx_t idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, idx_t idx) override;

  template <class T>
  RaidMapItem getAutoDeviceZone(T pos);
  template <class T>
  RaidMapItem getAutoDeviceZoneFromIdx(T idx);
  template <class T>
  idx_t getAutoDeviceZoneIdx(T pos);
  template <class T>
  T getAutoMappedDevicePos(T pos);

  ~RaidAutoZonedBlockDevice() override = default;
};

class RaidInfoBasic {
 public:
  RaidMode main_mode = RaidMode::RAID_NONE;
  uint32_t nr_devices = 0;
  // assert all devices are same in these fields
  uint32_t dev_block_size = 0; /* in bytes */
  uint32_t dev_zone_size = 0;  /* in blocks */
  uint32_t dev_nr_zones = 0;   /* in one device */

  void load(ZonedBlockDevice *zbd) {
    assert(sizeof(RaidInfoBasic) == sizeof(uint32_t) * 5);
    if (zbd->IsRAIDEnabled()) {
      auto be =
          dynamic_cast<AbstractRaidZonedBlockDevice *>(zbd->getBackend().get());
      if (!be) return;
      main_mode = be->getMainMode();
      nr_devices = be->nr_dev();
      dev_block_size = be->def_dev()->GetBlockSize();
      dev_zone_size = be->def_dev()->GetZoneSize();
      dev_nr_zones = be->def_dev()->GetNrZones();
    }
  }

  Status compatible(ZonedBlockDevice *zbd) const {
    if (!zbd->IsRAIDEnabled()) return Status::OK();
    auto be =
        dynamic_cast<AbstractRaidZonedBlockDevice *>(zbd->getBackend().get());
    if (!be) return Status::NotSupported("RAID Error", "cannot cast pointer");
    if (main_mode != be->getMainMode())
      return Status::Corruption(
          "RAID Error", "main_mode mismatch: superblock-raid" +
                            std::string(raid_mode_str(main_mode)) +
                            " != disk-raid" + raid_mode_str(be->getMainMode()));
    if (nr_devices != be->nr_dev())
      return Status::Corruption("RAID Error", "nr_devices mismatch");
    if (dev_block_size != be->def_dev()->GetBlockSize())
      return Status::Corruption("RAID Error", "dev_block_size mismatch");
    if (dev_zone_size != be->def_dev()->GetZoneSize())
      return Status::Corruption("RAID Error", "dev_zone_size mismatch");
    if (dev_nr_zones != be->def_dev()->GetNrZones())
      return Status::Corruption("RAID Error", "dev_nr_zones mismatch");
    return Status::OK();
  }
};

class RaidInfoAppend {
 public:
  RaidAutoZonedBlockDevice::device_zone_map_t device_zone_map;
  RaidAutoZonedBlockDevice::mode_map_t mode_map;
};
}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAID_AUTO_H
