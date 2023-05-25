//
// Created by chiro on 23-5-6.
//

#include "zone_raid1.h"

namespace AQUAFS_NAMESPACE {

Raid1ZonedBlockDevice::Raid1ZonedBlockDevice(
    const std::shared_ptr<Logger> &logger,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : AbstractRaidZonedBlockDevice(logger, RaidMode::RAID1,
                                   std::move(devices)) {
  syncBackendInfo();
}
std::unique_ptr<ZoneList> Raid1ZonedBlockDevice::ListZones() {
  return def_dev()->ListZones();
}
IOStatus Raid1ZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                      uint64_t *max_capacity) {
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Reset(start, offline, max_capacity);
    if (!s.ok()) return s;
  }
  return s;
}
IOStatus Raid1ZonedBlockDevice::Finish(uint64_t start) {
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Finish(start);
    if (!s.ok()) return s;
  }
  return s;
}
IOStatus Raid1ZonedBlockDevice::Close(uint64_t start) {
  IOStatus s;
  for (auto &&d : devices_) {
    s = d->Close(start);
    if (!s.ok()) return s;
  }
  return s;
}
int Raid1ZonedBlockDevice::Read(char *buf, int size, uint64_t pos,
                                bool direct) {
  int r = 0;
  for (auto &&d : devices_) {
    if ((r = d->Read(buf, size, pos, direct))) {
      return r;
    }
  }
  return r;
}
int Raid1ZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  int r = 0;
  for (auto &&d : devices_) {
    if ((r = d->Write(data, size, pos))) {
      return r;
    }
  }
  return r;
}
int Raid1ZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  int r = 0;
  for (auto &&d : devices_) r = d->InvalidateCache(pos, size);
  return r;
}
bool Raid1ZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  return def_dev()->ZoneIsSwr(zones, idx);
}
bool Raid1ZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  return def_dev()->ZoneIsOffline(zones, idx);
}
bool Raid1ZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                           unsigned int idx) {
  return def_dev()->ZoneIsWritable(zones, idx);
}
bool Raid1ZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  return def_dev()->ZoneIsActive(zones, idx);
}
bool Raid1ZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  return def_dev()->ZoneIsOpen(zones, idx);
}
uint64_t Raid1ZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  return def_dev()->ZoneStart(zones, idx);
}
uint64_t Raid1ZonedBlockDevice::ZoneMaxCapacity(
    std::unique_ptr<ZoneList> &zones, unsigned int idx) {
  return def_dev()->ZoneMaxCapacity(zones, idx);
}
uint64_t Raid1ZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                       unsigned int idx) {
  return def_dev()->ZoneWp(zones, idx);
}
void Raid1ZonedBlockDevice::syncBackendInfo() {
  AbstractRaidZonedBlockDevice::syncBackendInfo();
  // do nothing
}
}  // namespace AQUAFS_NAMESPACE
