//
// Created by chiro on 23-5-6.
//

#ifndef ROCKSDB_ZONE_RAID_AUTO_H
#define ROCKSDB_ZONE_RAID_AUTO_H

#include <gflags/gflags.h>

#include "zone_raid.h"
#include "zone_raid_allocator.h"

DECLARE_string(raid_auto_default);

namespace aquafs {
class RaidAutoZonedBlockDevice : public AbstractRaidZonedBlockDevice {
 public:
  // template <typename K, typename V>
  // using map_use = ZoneRaidAllocator::map_use<K, V>;
  using device_zone_map_t = ZoneRaidAllocator::device_zone_map_t;
  using mode_map_t = ZoneRaidAllocator::mode_map_t;
  using raid_zone_t = struct zbd_zone;

  ZoneRaidAllocator allocator;

 private:
  // auto-raid: manually managed zone info
  std::unique_ptr<raid_zone_t> a_zones_{};
  zbd_zone *zone_info(idx_t idx) { return a_zones_.get() + idx; }

  void flush_zone_info();

  void syncBackendInfo() override;

 public:
  explicit RaidAutoZonedBlockDevice(
      const std::shared_ptr<Logger> &logger,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices);

  void layout_update(device_zone_map_t &&device_zone, mode_map_t &&mode_map);
  void layout_setup(device_zone_map_t &&device_zone, mode_map_t &&mode_map);

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

  Status ScanAndHandleOffline();

  ~RaidAutoZonedBlockDevice() override = default;

  void setZoneOffline(unsigned int idx, unsigned int idx2,
                      bool offline) override;
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
#ifdef ROCKSDB_USE_RTTI
      auto be =
          dynamic_cast<AbstractRaidZonedBlockDevice *>(zbd->getBackend().get());
      if (!be) return;
#else
      auto be = (AbstractRaidZonedBlockDevice *)(zbd->getBackend().get());
#endif
      main_mode = be->getMainMode();
      nr_devices = be->nr_dev();
      dev_block_size = be->def_dev()->GetBlockSize();
      dev_zone_size = be->def_dev()->GetZoneSize();
      dev_nr_zones = be->def_dev()->GetNrZones();
    }
  }

  Status compatible(ZonedBlockDevice *zbd) const {
    if (!zbd->IsRAIDEnabled()) return Status::OK();
#ifdef ROCKSDB_USE_RTTI
    auto be =
        dynamic_cast<AbstractRaidZonedBlockDevice *>(zbd->getBackend().get());
    if (!be) return Status::NotSupported("RAID Error", "cannot cast pointer");
#else
    auto be = (AbstractRaidZonedBlockDevice *)(zbd->getBackend().get());
#endif
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
}  // namespace AQUAFS_NAMESPACE

#endif  // ROCKSDB_ZONE_RAID_AUTO_H
