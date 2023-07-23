//
// Created by lyt on 7/3/23.
//
#include "zone_raid5.h"

#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <ostream>

#include "../zbd_aquafs.h"
#include "zone_raid.h"

#ifdef AQUAFS_RAID_URING
#include "../../liburing4cpp/include/liburing/io_service.hpp"
#endif

namespace aquafs {
IOStatus Raid5ZoneBlockDevice::Open(bool readonly, bool exclusive,
                                    unsigned int *max_active_zones,
                                    unsigned int *max_open_zones) {
  auto s = AbstractRaidZonedBlockDevice::Open(readonly, exclusive,
                                              max_active_zones, max_open_zones);
  if (!s.ok()) return s;
  allocator.setInfo(nr_dev(), nr_zones_);
  // scan offline zones
  for (idx_t d = 0; d < nr_dev(); d++)
    for (idx_t z = 0; z < nr_zones_; z++) {
      auto zones = devices_[d]->ListZones();
      if (devices_[d]->ZoneIsOffline(zones, z)) {
        allocator.setOffline(d, z);
      }
    }
  for (idx_t idx = 0; idx < AQUAFS_META_ZONES; idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      allocator.addMapping(idx * nr_dev() + i, 0, idx * nr_dev() + i);
    allocator.setMappingMode(idx, RaidMode::RAID_NONE);
  }
  for (idx_t idx = AQUAFS_META_ZONES; idx < def_dev()->GetNrZones(); idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      allocator.addMapping(idx * nr_dev() + i, i, idx * nr_dev() + i);
    allocator.setMappingMode(idx, RaidMode::RAID5);
  }
  // std::cout << "device 2 mapping mode : "
  //           << raid_mode_str(allocator.mode_map_[2].mode) << std::endl;
  return s;
}
void Raid5ZoneBlockDevice::syncBackendInfo() {
  for (idx_t idx = 0; idx < AQUAFS_META_ZONES; idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      allocator.addMapping(idx * nr_dev() + i, 0, idx * nr_dev() + i);
    allocator.setMappingMode(idx, RaidMode::RAID_NONE);
  }
  // add raid5 label
  // std::cout << "last raid5 dev_id : " << def_dev()->GetNrZones() - 1
  // << std::endl;
  for (idx_t idx = AQUAFS_META_ZONES; idx < def_dev()->GetNrZones(); idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      allocator.addMapping(idx * nr_dev() + i, i, idx * nr_dev() + i);
    allocator.setMappingMode(idx, RaidMode::RAID5);
  }
  // std::cout << "mapping in 5 is: " <<
  // raid_mode_str(allocator.mode_map_[5].mode)
  // << std::endl;
  AbstractRaidZonedBlockDevice::syncBackendInfo();
  // std::cout << "in sync before:" << zone_sz_ << std::endl;
  zone_sz_ *= nr_dev() - 1;
  // std::cout << "in sync after:" << zone_sz_ << std::endl;
}

Raid5ZoneBlockDevice::Raid5ZoneBlockDevice(
    const std::shared_ptr<Logger> &logger,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : AbstractRaidZonedBlockDevice(logger, RaidMode::RAID5,
                                   std::move(devices)) {
  // assert(devices.size() >= 2); 这里已经被移动到底层了
  syncBackendInfo();
}

std::unique_ptr<ZoneList> Raid5ZoneBlockDevice::ListZones() {
  std::unique_ptr<ZoneList> zones = def_dev()->ListZones();
  if (zones) {
    auto nr_zones = zones->ZoneCount();
    auto data = new struct zbd_zone[nr_zones];
    auto ptr = data;
    memcpy(data, zones->GetData(), sizeof(struct zbd_zone) * nr_zones);
    for (decltype(nr_zones) i = 0; i < nr_zones; i++) {
      ptr->start *= nr_dev() - 1;
      ptr->len *= nr_dev() - 1;
      ptr->capacity *= nr_dev() - 1;
      ptr++;
    }
    return std::make_unique<ZoneList>(data, nr_zones);
  } else {
    return nullptr;
  }
}

IOStatus Raid5ZoneBlockDevice::Reset(uint64_t start, bool *offline,
                                     uint64_t *max_capacity) {
  // std::cout << "start:" << start << ",zone_size:" << GetZoneSize()
  // << ",block_size:" << GetBlockSize() << std::endl;
  assert(start % GetBlockSize() == 0);
  // assert(start % GetZoneSize() == 0);
  auto s = start / (nr_dev() - 1);
  IOStatus r{};
  for (auto &&d : devices_) {
    r = d->Reset(s, offline, max_capacity);
    if (r.ok()) {
      *max_capacity *= nr_dev() - 1;
    }
  }
  return r;
}

IOStatus Raid5ZoneBlockDevice::Finish(uint64_t start) {
  assert(start % GetBlockSize() == 0);
  assert(start % GetZoneSize() == 0);
  auto s = start / (nr_dev() - 1);
  // auto r = devices_[idx_dev]->Close(s);
  IOStatus r{};
  for (auto &&d : devices_) {
    r = d->Finish(s);
    if (!r.ok()) {
      return r;
    }
  }
  return r;
}

IOStatus Raid5ZoneBlockDevice::Close(uint64_t start) {
  assert(start % GetBlockSize() == 0);
  // assert(start % GetZoneSize() == 0);
  auto s = start / (nr_dev() - 1);
  // auto r = devices_[idx_dev]->Close(s);
  IOStatus r{};
  for (auto &&d : devices_) {
    r = d->Close(s);
    if (!r.ok()) {
      return r;
    }
  }
  return r;
}

int Raid5ZoneBlockDevice::Read(char *buf, int size, uint64_t pos, bool direct) {
  // raid5 check
  int zone_start_index = pos / zone_sz_;
  int zone_end_index = (pos + size - 1) / zone_sz_;
  int dev_zone_size = this->GetZoneSize();
  int check_value = raid5_check(zone_start_index, zone_end_index, direct);
  std::cout << "check_value : " << check_value << std::endl;
#ifndef AQUAFS_RAID_URING
  int sz_read = 0;
  int r = 0;
  // multi-zones read
  if (static_cast<decltype(dev_zone_size)>(size) > dev_zone_size) {
    std::cout << "multi zone read" << std::endl;
    // read check
    if (check_value == -1) return -1;
    // split read range as blocks
    while (size > 0) {
      auto req_size = std::min(
          size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
      r = devices_[get_idx_dev(pos)]->Read(buf, req_size, req_pos(pos), direct);
      if (r > 0) {
        size -= r;
        sz_read += r;
        buf += r;
        pos += r;
      } else {
        return r;
      }
    }
    return sz_read;
  } else {  // one zone read
    std::cout << "one zone read" << std::endl;
    assert(static_cast<decltype(zone_sz_)>(size) <= zone_sz_);
    if (check_value == -1) {  // zone restore
      int device_id = pos % zone_sz_ / zone_sz_;
      std::cout << "Raid5 Restore" << std::endl;
      int s = raid5_restore(device_id, pos % dev_zone_size, buf, direct);
      std::cout << "Read S : " << s << std::endl;
      if (s <= 0) return -1;
    }

    auto mode_item = allocator.mode_map_[pos / zone_sz_];
    if (mode_item.mode == RaidMode::RAID_NONE) {
      std::cout << "Raid None" << std::endl;
      auto m = getAutoDeviceZone(pos);
      auto mapped_pos = getAutoMappedDevicePos(pos);
      // std::cout << "device_id : " << m.device_idx
      // << ",Read mapped_pos : " << mapped_pos << std::endl;
      r = devices_[m.device_idx]->Read(buf, size, mapped_pos, direct);
      // Info(logger_,
      //      "RAID-A: WRITE raid%s mapping pos=%lx to mapped_pos=%lx, size=%x,
      //      " "dev=%x, zone=%x; r=%x", raid_mode_str(mode_item.mode), pos,
      //      mapped_pos, size, m.device_idx, m.zone_idx, r);
      return r;
    } else {
      while (size > 0) {
        auto req_size = std::min(
            size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
        r = devices_[get_idx_dev(pos)]->Read(buf, req_size, req_pos(pos),
                                             direct);
        if (r > 0) {
          size -= r;
          sz_read += r;
          buf += r;
          pos += r;
        } else {
          return r;
        }
      }
      return sz_read;
    }
  }
#else
  uio::io_service service;
  // split read range as blocks
  int sz_read = 0;
  using req_item_t = std::tuple<int, char *, uint64_t, off_t>;
  std::vector<req_item_t> requests;
  std::vector<ZbdlibBackend *> bes(nr_dev());
  for (decltype(nr_dev()) i = 0; i < nr_dev() - 1; i++) {
#ifdef ROCKSDB_USE_RTTI
    bes[i] = dynamic_cast<ZbdlibBackend *>(devices_[i].get());
    assert(bes[i] != nullptr);
#else
    bes[i] = (ZbdlibBackend *)(devices_[i].get());
#endif
  }
  while (size > 0) {
    auto req_size =
        std::min(size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
    if (req_size == 0) break;
    auto be = bes[get_idx_dev(pos)];
    assert(be != nullptr);
    int fd = direct ? be->read_direct_f_ : be->read_f_;
    requests.emplace_back(fd, buf, req_size, req_pos(pos));
    size -= req_size;
    sz_read += req_size;
    buf += req_size;
    pos += req_size;
  }
  service.run([&]() -> uio::task<> {
    std::vector<uio::task<int>> futures;
    for (const auto &req : requests) {
      uint8_t flags = 0;
      // if (req != *requests.cend()) flags |= IOSQE_IO_LINK;
      futures.emplace_back(service.read(std::get<0>(req), std::get<1>(req),
                                        std::get<2>(req), std::get<3>(req),
                                        flags) |
                           uio::panic_on_err("failed to read!", true));
    }
    for (auto &&fut : futures) co_await fut;
  }());
  // raid5 check

#ifdef AQUAFS_SIM_DELAY
  delay_us(calculate_delay_us(size / requests.size()));
#endif
  return sz_read;
#endif

  return 0;
}

int Raid5ZoneBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  int zone_start_index = pos / zone_sz_;
  int zone_end_index = (pos + size - 1) / zone_sz_;
  int dev_zone_size = this->GetZoneSize();
  std::cout << "write [start index:" << zone_start_index
            << ",end index:" << zone_end_index << "]" << std::endl;
#ifndef AQUAFS_RAID_URING
  int sz_written = 0;
  int r = 0;
  if (static_cast<decltype(dev_zone_size)>(size) > dev_zone_size) {
    // split read range as blocks
    while (size > 0) {
      // std::cout << "raid5_write : 1" << std::endl;
      auto req_size = std::min(
          size, static_cast<uint32_t>(GetBlockSize() - pos % GetBlockSize()));
      // std::cout << "dev id:" << get_idx_dev(pos) << ", size:" << req_size
      // << ", pos:" << pos << std::endl;
      r = devices_[get_idx_dev(pos)]->Write(data, req_size, req_pos(pos));
      // std::cout << "raid5_write : 2" << std::endl;
      if (r > 0) {
        size -= r;
        sz_written += r;
        data += r;
        pos += r;
      } else {
        return r;
      }
    }
  } else {
    // std::cout << "mapping in mode_map_ : " << pos / zone_sz_ << std::endl;
    auto mode_item = allocator.mode_map_[pos / zone_sz_];
    if (mode_item.mode == RaidMode::RAID_NONE) {
      auto m = getAutoDeviceZone(pos);
      auto mapped_pos = getAutoMappedDevicePos(pos);
      r = devices_[m.device_idx]->Write(data, size, mapped_pos);
      std::cout << "device_id : " << m.device_idx
                << ",write to mapped_pos : " << mapped_pos << std::endl;
      // Info(logger_,
      //      "RAID-NONE: WRITE raid%s mapping pos=%lx to mapped_pos=%lx,
      //      size=%x, " "dev=%x, zone=%x; r=%x",
      //      raid_mode_str(mode_item.mode), pos, mapped_pos, size,
      //      m.device_idx, m.zone_idx, r);
      return r;
    } else {
      while (size > 0) {
        std::cout << "raid5_write : 1" << std::endl;
        auto req_size = std::min(
            size, static_cast<uint32_t>(GetBlockSize() - pos % GetBlockSize()));
        std::cout << "dev id:" << get_idx_dev(pos) << ", size:" << req_size
                  << ", pos:" << pos << ",req_pos :" << req_pos(pos)
                  << ", zone number:" << pos / zone_sz_ << std::endl;
        r = devices_[get_idx_dev(pos)]->Write(data, req_size, req_pos(pos));
        // std::cout << "raid5_write : 2" << std::endl;
        if (r > 0) {
          size -= r;
          sz_written += r;
          data += r;
          pos += r;
        } else {
          return r;
        }
      }
    }
  }
  // raid5 update direct??
  if (raid5_update(zone_start_index, zone_end_index, false) == 0) return -1;
  return sz_written;
#else
  uio::io_service service;
  uint32_t sz_written = 0;
  using req_item_t = std::tuple<int, char *, uint64_t, off_t>;
  std::vector<req_item_t> requests;
  std::vector<ZbdlibBackend *> bes(nr_dev());
  for (decltype(nr_dev()) i = 0; i < nr_dev() - 1; i++) {
#ifdef ROCKSDB_USE_RTTI
    bes[i] = dynamic_cast<ZbdlibBackend *>(devices_[i].get());
    assert(bes[i] != nullptr);
#else
    bes[i] = (ZbdlibBackend *)(devices_[i].get());
#endif
  }
  while (size > 0) {
    auto req_size =
        std::min(size, static_cast<int>(GetBlockSize() - pos % GetBlockSize()));
    if (req_size == 0) break;
    auto be = bes[get_idx_dev(pos)];
    assert(be != nullptr);
    int fd = be->write_f_;
    requests.emplace_back(fd, data, req_size, req_pos(pos));
    size -= req_size;
    sz_written += req_size;
    data += req_size;
    pos += req_size;
  }
  service.run([&]() -> uio::task<> {
    std::vector<uio::task<int>> futures;
    for (const auto &req : requests) {
      uint8_t flags = 0;
      // if (req != *requests.cend()) flags |= IOSQE_IO_LINK;
      futures.emplace_back(service.write(std::get<0>(req), std::get<1>(req),
                                         std::get<2>(req), std::get<3>(req),
                                         flags) |
                           uio::panic_on_err("failed to read!", true));
    }
    for (auto &&fut : futures) co_await fut;
  }());
  // raid5 update

#ifdef AQUAFS_SIM_DELAY
  delay_us(calculate_delay_us(size / requests.size()));
#endif
  return sz_written;
#endif
  return 0;
}

int Raid5ZoneBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  assert(size % GetBlockSize() == 0);
  for (size_t i = 0; i < nr_dev(); i++) {
    devices_[i]->InvalidateCache(req_pos(pos), size / (nr_dev() - 1));
  }
  return 0;
}

bool Raid5ZoneBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                     unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsSwr(z, idx);
}

bool Raid5ZoneBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsOffline(z, idx);
}

bool Raid5ZoneBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                          unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsWritable(z, idx);
}

bool Raid5ZoneBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                        unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsActive(z, idx);
}

bool Raid5ZoneBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneIsOpen(z, idx);
}

uint64_t Raid5ZoneBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                         unsigned int idx) {
  return std::accumulate(devices_.begin(), devices_.end(),
                         static_cast<uint64_t>(0), [&](uint64_t sum, auto &d) {
                           auto z = d->ListZones();
                           return sum + d->ZoneStart(z, idx);
                         });
}

uint64_t Raid5ZoneBlockDevice::ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                               unsigned int idx) {
  auto z = def_dev()->ListZones();
  return def_dev()->ZoneMaxCapacity(z, idx) * (nr_dev() - 1);
  return 0;
}

uint64_t Raid5ZoneBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                      unsigned int idx) {
  return std::accumulate(devices_.begin(), devices_.end(),
                         static_cast<uint64_t>(0), [&](uint64_t sum, auto &d) {
                           auto z = d->ListZones();
                           return sum + d->ZoneWp(z, idx);
                         });
}

int Raid5ZoneBlockDevice::raid5_check(int zone_start_index, int zone_end_index,
                                      bool direct) {
  // std::cout << "raid5_check [start_index:" << zone_start_index
  // << ", end_index:" << zone_end_index << "]" << std::endl;
  int r = 0;
  char *buf = new char[def_dev()->GetZoneSize()];
  char *data = new char[def_dev()->GetZoneSize()];
  while (zone_start_index <= zone_end_index) {
    memset(buf, 0, sizeof(def_dev()->GetZoneSize()));
    memset(data, 0, sizeof(def_dev()->GetZoneSize()));
    for (decltype(nr_dev()) i = 0; i < nr_dev() - 1; i++) {
      devices_[i]->Read(data, def_dev()->GetZoneSize(),
                        zone_start_index * def_dev()->GetZoneSize(), direct);
      for (uint64_t j = 0; j < def_dev()->GetZoneSize(); j++) {
        buf[j] ^= data[j];
      }
    }
    r = devices_[nr_dev() - 1]->Read(
        data, def_dev()->GetZoneSize(),
        zone_start_index * def_dev()->GetZoneSize(), direct);
    if (r <= 0) {
      delete[] buf;
      delete[] data;
      return 0;
    }
    if (memcmp(buf, data, sizeof(def_dev()->GetZoneSize())) != 0) {
      return 0;
    }
    zone_start_index++;
  }
  delete[] buf;
  delete[] data;
  return 1;
}

int Raid5ZoneBlockDevice::raid5_update(int zone_start_index, int zone_end_index,
                                       bool direct) {
  int r = 0;
  char *buf = new char[def_dev()->GetZoneSize()];
  char *data = new char[def_dev()->GetZoneSize()];
  while (zone_start_index <= zone_end_index) {
    memset(buf, 0, sizeof(def_dev()->GetZoneSize()));
    memset(data, 0, sizeof(def_dev()->GetZoneSize()));
    for (decltype(nr_dev()) i = 0; i < nr_dev() - 1; i++) {
      r = devices_[i]->Read(data, def_dev()->GetZoneSize(),
                            zone_start_index * def_dev()->GetZoneSize(),
                            direct);
      for (uint64_t j = 0; j < def_dev()->GetZoneSize(); j++) {
        buf[j] ^= data[j];
      }
    }
    r = devices_[nr_dev() - 1]->Write(
        data, def_dev()->GetZoneSize(),
        zone_start_index * def_dev()->GetZoneSize());
    if (r <= 0) {
      delete[] buf;
      delete[] data;
      return 0;
    }

    zone_start_index++;
  }
  delete[] buf;
  delete[] data;
  return 1;
}

int Raid5ZoneBlockDevice::raid5_restore(int device_id, int zone_index,
                                        char *restore_data, bool direct) {
  // find the offline zone_index
  memset(restore_data, 0, def_dev()->GetZoneSize());
  char *data = new char[def_dev()->GetZoneSize()];
  int r = 0;
  for (decltype(nr_dev()) i = 0; i < nr_dev(); i++) {
    if (i == static_cast<unsigned long>(device_id)) continue;
    r = devices_[i]->Read(data, def_dev()->GetZoneSize(),
                          zone_index * def_dev()->GetZoneSize(), direct);
    if (r <= 0) {
      delete[] data;
      return -1;
    }
    for (uint64_t j = 0; j < def_dev()->GetZoneSize(); j++) {
      restore_data[j] ^= data[j];
    }
  }
  r = devices_[device_id]->Write(data, def_dev()->GetZoneSize(),
                                 zone_index * def_dev()->GetZoneSize());
  delete[] data;
  if (r <= 0) {
    return 0;
  }
  return 1;
}

template <class T>
RaidMapItem Raid5ZoneBlockDevice::getAutoDeviceZone(T pos) {
  return allocator.device_zone_map_[getAutoDeviceZoneIdx(pos)][0];
}
template <class T>
idx_t Raid5ZoneBlockDevice::getAutoDeviceZoneIdx(T pos) {
  auto raid_zone_idx = pos / zone_sz_;
  auto raid_zone_inner_idx =
      (pos - (raid_zone_idx * zone_sz_)) / def_dev()->GetZoneSize();
  auto raid_block_idx = pos / block_sz_;
  // index of block in this raid zone
  auto raid_zone_block_idx =
      raid_block_idx - (raid_zone_idx * (zone_sz_ / block_sz_));
  auto mode_item = allocator.mode_map_[raid_zone_idx];
  if (mode_item.mode == RaidMode::RAID_NONE ||
      mode_item.mode == RaidMode::RAID_C || mode_item.mode == RaidMode::RAID1) {
    return raid_zone_idx * (nr_dev() - 1) + raid_zone_inner_idx;
  } else if (mode_item.mode == RaidMode::RAID0) {
    // Info(logger_, "\t[pos=%x] raid_zone_idx=%lx raid_zone_block_idx = %lx",
    //      static_cast<uint32_t>(pos), raid_zone_idx, raid_zone_block_idx);
    return raid_zone_idx * (nr_dev() - 1) + raid_zone_block_idx % nr_dev();
  }
  Warn(logger_, "Cannot locate device zone at pos=%x",
       static_cast<uint32_t>(pos));
  return {};
}

template <class T>
T Raid5ZoneBlockDevice::getAutoMappedDevicePos(T pos) {
  auto raid_zone_idx = pos / zone_sz_;
  RaidMapItem map_item = getAutoDeviceZone(pos);
  auto mode_item = allocator.mode_map_[raid_zone_idx];
  auto blk_idx = pos / block_sz_;
  // if (mode_item.mode == RaidMode::RAID_NONE) {
  //   return pos;
  // } else
  if (mode_item.mode == RaidMode::RAID0) {
    auto base = map_item.zone_idx * def_dev()->GetZoneSize();
    auto nr_blk_in_raid_zone = zone_sz_ / block_sz_;
    auto blk_idx_raid_zone = blk_idx % nr_blk_in_raid_zone;
    auto blk_idx_dev_zone = blk_idx_raid_zone / nr_dev();
    auto offset_in_blk = pos % block_sz_;
    auto offset_in_zone = blk_idx_dev_zone * block_sz_;
    return base + offset_in_zone + offset_in_blk;
  } else if (mode_item.mode == RaidMode::RAID1) {
    // FIXME
    return map_item.zone_idx * def_dev()->GetZoneSize() + pos % zone_sz_;
  } else {
    return map_item.zone_idx * def_dev()->GetZoneSize() +
           ((blk_idx % (def_dev()->GetZoneSize() / block_sz_)) * block_sz_) +
           pos % block_sz_;
  }
}
}  // namespace aquafs