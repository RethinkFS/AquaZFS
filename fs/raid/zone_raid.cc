//
// Created by chiro on 23-4-28.
//

#include "zone_raid.h"

#include <memory>
#include <queue>
#include <utility>

#include "base/io_status.h"

namespace aquafs {

const char *raid_mode_str(RaidMode mode) {
  switch (mode) {
    case RaidMode::RAID0:
      return "0";
    case RaidMode::RAID1:
      return "1";
    case RaidMode::RAID5:
      return "5";
    case RaidMode::RAID6:
      return "6";
    case RaidMode::RAID10:
      return "10";
    case RaidMode::RAID_A:
      return "a";
    case RaidMode::RAID_C:
      return "c";
    case RaidMode::RAID_NONE:
      return "n";
    default:
      return "UNKNOWN";
  }
}
RaidMode raid_mode_from_str(const std::string &str) {
  if (str == "0") {
    return RaidMode::RAID0;
  } else if (str == "1") {
    return RaidMode::RAID1;
  } else if (str == "5") {
    return RaidMode::RAID5;
  } else if (str == "6") {
    return RaidMode::RAID6;
  } else if (str == "10") {
    return RaidMode::RAID10;
  } else if (str == "A" || str == "a" || str == "-a" || str == "-A") {
    return RaidMode::RAID_A;
  } else if (str == "C" || str == "c" || str == "-c" || str == "-C") {
    return RaidMode::RAID_C;
  }
  return RaidMode::RAID_A;
}

class RaidConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  RaidConsoleLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  void Logv(const char *format, va_list ap) override {
    lock_.lock();
    printf("[RAID] ");
    vprintf(format, ap);
    printf("\n");
    fflush(stdout);
    lock_.unlock();
  }

  std::mutex lock_;
};

AbstractRaidZonedBlockDevice::AbstractRaidZonedBlockDevice(
    const std::shared_ptr<Logger> &logger, RaidMode main_mode,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : logger_(logger), main_mode_(main_mode), devices_(std::move(devices)) {
  if (!logger_) logger_.reset(new RaidConsoleLogger());
  assert(!devices_.empty());
  Info(logger_, "RAID Mode: raid%s Devices: ", raid_mode_str(main_mode_));
  assert(this->IsRAIDEnabled());
  for (auto &&d : devices_) Info(logger_, "  %s", d->GetFilename().c_str());
}

IOStatus AbstractRaidZonedBlockDevice::Open(bool readonly, bool exclusive,
                                            unsigned int *max_active_zones,
                                            unsigned int *max_open_zones) {
  Info(logger_, "Open(readonly=%s, exclusive=%s)",
       std::to_string(readonly).c_str(), std::to_string(exclusive).c_str());
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Open(readonly, exclusive, max_active_zones, max_open_zones);
    if (!s.ok()) return s;
    Info(logger_,
         "%s opened, sz=%lx, nr_zones=%x, zone_sz=%lx blk_sz=%x "
         "max_active_zones=%x, max_open_zones=%x",
         d->GetFilename().c_str(), d->GetNrZones() * d->GetZoneSize(),
         d->GetNrZones(), d->GetZoneSize(), d->GetBlockSize(),
         *max_active_zones, *max_open_zones);
    assert(d->GetNrZones() == def_dev()->GetNrZones());
    assert(d->GetZoneSize() == def_dev()->GetZoneSize());
    assert(d->GetBlockSize() == def_dev()->GetBlockSize());
  }
  syncBackendInfo();
  Info(logger_, "after Open(): nr_zones=%x, zone_sz=%lx blk_sz=%x", nr_zones_,
       zone_sz_, block_sz_);
  return s;
}
void AbstractRaidZonedBlockDevice::syncBackendInfo() {
  total_nr_devices_zones_ = std::accumulate(
      devices_.begin(), devices_.end(), 0,
      [](int sum, const std::unique_ptr<ZonedBlockDeviceBackend> &dev) {
        return sum + dev->GetNrZones();
      });
  block_sz_ = def_dev()->GetBlockSize();
  zone_sz_ = def_dev()->GetZoneSize();
  nr_zones_ = def_dev()->GetNrZones();
}
std::string AbstractRaidZonedBlockDevice::GetFilename() {
  std::string name = std::string("raid") + raid_mode_str(main_mode_) + ":";
  for (auto p = devices_.begin(); p != devices_.end(); p++) {
    name += (*p)->GetFilename();
    if (p + 1 != devices_.end()) name += ",";
  }
  return name;
}
bool AbstractRaidZonedBlockDevice::IsRAIDEnabled() const { return true; }
RaidMode AbstractRaidZonedBlockDevice::getMainMode() const {
  return main_mode_;
}
}  // namespace aquafs