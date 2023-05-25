//
// Created by chiro on 23-5-6.
//

#include "zone_raidc.h"

#include <cstdint>
namespace AQUAFS_NAMESPACE {
RaidCZonedBlockDevice::RaidCZonedBlockDevice(
    const std::shared_ptr<Logger> &logger,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : AbstractRaidZonedBlockDevice(logger, RaidMode::RAID_C,
                                   std::move(devices)) {}
void RaidCZonedBlockDevice::syncBackendInfo() {
  AbstractRaidZonedBlockDevice::syncBackendInfo();
  nr_zones_ = total_nr_devices_zones_;
}
std::unique_ptr<ZoneList> RaidCZonedBlockDevice::ListZones() {
  std::vector<std::unique_ptr<ZoneList>> list;
  for (auto &&dev : devices_) {
    auto zones = dev->ListZones();
    if (zones) {
      list.emplace_back(std::move(zones));
    }
  }
  // merge zones
  auto nr_zones = std::accumulate(
      list.begin(), list.end(), 0,
      [](int sum, auto &zones) { return sum + zones->ZoneCount(); });
  auto data = new struct zbd_zone[nr_zones];
  auto ptr = data;
  for (auto &&zones : list) {
    auto nr = zones->ZoneCount();
    memcpy(ptr, zones->GetData(), sizeof(struct zbd_zone) * nr);
    ptr += nr;
  }
  return std::make_unique<ZoneList>(data, nr_zones);
}
IOStatus RaidCZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                      uint64_t *max_capacity) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > start) {
      return d->Reset(start, offline, max_capacity);
    } else {
      start -= sz;
    }
  }
  return IOStatus::IOError();
}
IOStatus RaidCZonedBlockDevice::Finish(uint64_t start) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > start) {
      return d->Finish(start);
    } else {
      start -= sz;
    }
  }
  return unsupported;
}
IOStatus RaidCZonedBlockDevice::Close(uint64_t start) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > start) {
      return d->Close(start);
    } else {
      start -= sz;
    }
  }
  return unsupported;
}
int RaidCZonedBlockDevice::Read(char *buf, int size, uint64_t pos,
                                bool direct) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > pos) {
      return d->Read(buf, size, pos, direct);
    } else {
      pos -= sz;
    }
  }
  return -1;
}
int RaidCZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > pos) {
      return d->Write(data, size, pos);
    } else {
      pos -= sz;
    }
  }
  return -1;
}
int RaidCZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  for (auto &&d : devices_) {
    auto sz = d->GetNrZones() * d->GetZoneSize();
    if (sz > pos) {
      return d->InvalidateCache(pos, size);
    } else {
      pos -= sz;
    }
  }
  return 0;
}
bool RaidCZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      return d->ZoneIsSwr(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return false;
}
bool RaidCZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      // FIXME: optimize list-zones
      auto z = d->ListZones();
      return d->ZoneIsOffline(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return false;
}
bool RaidCZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                           unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      return d->ZoneIsWritable(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return false;
}
bool RaidCZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      return d->ZoneIsActive(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return false;
}
bool RaidCZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      return d->ZoneIsOpen(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return false;
}
uint64_t RaidCZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      // FIXME: is it right?
      return d->ZoneStart(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return 0;
}
uint64_t RaidCZonedBlockDevice::ZoneMaxCapacity(
    std::unique_ptr<ZoneList> &zones, unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      return d->ZoneMaxCapacity(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return 0;
}
uint64_t RaidCZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  for (auto &&d : devices_) {
    if (d->GetNrZones() > idx) {
      auto z = d->ListZones();
      // FIXME: is it right?
      return d->ZoneWp(z, idx);
    } else {
      idx -= d->GetNrZones();
    }
  }
  return 0;
}
}  // namespace AQUAFS_NAMESPACE