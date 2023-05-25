#ifndef HEAD_ZONE_RAID_H
#define HEAD_ZONE_RAID_H

#include <map>
#include <memory>
#include <numeric>
#include <unordered_map>

#include "../zbd_aquafs.h"
#include "base/io_status.h"


namespace aquafs {

class RaidConsoleLogger;

enum class RaidMode : uint32_t {
  // AquaFS: No RAID, just use the first backend device
  RAID_NONE = 0,
  RAID0,
  RAID1,
  RAID5,
  RAID6,
  RAID10,
  // AquaFS: Concat-RAID
  RAID_C,
  // AquaFS: Auto-RAID
  RAID_A
};

__attribute__((__unused__)) const char *raid_mode_str(RaidMode mode);
__attribute__((__unused__)) RaidMode raid_mode_from_str(const std::string &str);

using idx_t = unsigned int;

class RaidMapItem {
 public:
  // device index
  idx_t device_idx{};
  // zone index on this device
  idx_t zone_idx{};
  // when invalid, ignore this <device_idx, zone_idx> in the early record
  uint16_t invalid{};

  Status DecodeFrom(Slice *input);
};

class RaidModeItem {
 public:
  RaidMode mode = RaidMode::RAID_NONE;
  // extra option for raid mode, for example: n extra zones for raid5
  uint32_t option{};

  Status DecodeFrom(Slice *input);
};

class AbstractRaidZonedBlockDevice : public ZonedBlockDeviceBackend {
 public:
  explicit AbstractRaidZonedBlockDevice(
      const std::shared_ptr<Logger> &logger, RaidMode main_mode,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices);
  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones) override;

  using raid_zone_t = struct zbd_zone;

  [[nodiscard]] ZonedBlockDeviceBackend *def_dev() const {
    return devices_.begin()->get();
  }

  [[nodiscard]] auto nr_dev() const { return devices_.size(); }

  std::string GetFilename() override;
  [[nodiscard]] bool IsRAIDEnabled() const override;
  [[nodiscard]] RaidMode getMainMode() const;

 protected:
  std::shared_ptr<Logger> logger_{};
  RaidMode main_mode_{};
  std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> devices_{};
  // how many zones in total of all devices
  uint32_t total_nr_devices_zones_{};
  const IOStatus unsupported = IOStatus::NotSupported("Raid unsupported");

  virtual void syncBackendInfo();

  template <class T>
  T nr_dev_t() const {
    return static_cast<T>(devices_.size());
  }
};
};  // namespace aquafs

#endif  // HEAD_ZONE_RAID_H