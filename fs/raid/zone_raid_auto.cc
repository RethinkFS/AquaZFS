//
// Created by chiro on 23-5-6.
//

#include "zone_raid_auto.h"

#include <gflags/gflags.h>

#include <memory>
#include <numeric>
#include <queue>
#include <utility>

#ifdef AQUAFS_RAID_URING
#include "../../liburing4cpp/include/liburing/io_service.hpp"
#endif
#include "../aquafs_utils.h"
#include "../zbdlib_aquafs.h"
#include "rocksdb/io_status.h"
#include "../../base/coding.h"

DEFINE_string(raid_auto_default, "1", "Default RAID mode for auto-raid");

namespace aquafs {

/**
 * @brief Construct a new Raid Zoned Block Device object
 * @param mode main mode. RAID_A for auto-raid
 * @param devices all devices under management
 */
RaidAutoZonedBlockDevice::RaidAutoZonedBlockDevice(
    const std::shared_ptr<Logger> &logger,
    std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices)
    : AbstractRaidZonedBlockDevice(logger, RaidMode::RAID_A,
                                   std::move(devices)) {
  // create temporal device map: AQUAFS_META_ZONES in the first device is used
  // as meta zones, and marked as RAID_NONE; others are marked as RAID_C
  for (idx_t idx = 0; idx < AQUAFS_META_ZONES; idx++) {
    for (size_t i = 0; i < nr_dev(); i++)
      allocator.addMapping(idx * nr_dev() + i, 0, idx * nr_dev() + i);
    allocator.setMappingMode(idx, RaidMode::RAID_NONE);
  }
  syncBackendInfo();
}

IOStatus RaidAutoZonedBlockDevice::Open(bool readonly, bool exclusive,
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
  // allocate default layout
  a_zones_.reset(new raid_zone_t[nr_zones_]);
  memset(a_zones_.get(), 0, sizeof(raid_zone_t) * nr_zones_);
  const auto target_default_raid =
      raid_mode_from_str(FLAGS_raid_auto_default);
  Info(logger_, "target_default_raid = %s", FLAGS_raid_auto_default.c_str());
  if (target_default_raid == RaidMode::RAID0) {
    // spare some free zones for dynamic allocation
    for (idx_t idx = AQUAFS_META_ZONES; idx < nr_zones_ / 2; idx++) {
      auto status = allocator.createMapping(idx);
      allocator.setMappingMode(idx, target_default_raid);
      if (!status.ok()) {
        Error(logger_, "Failed to create mapping for zone %x", idx);
      }
    }
  } else if (target_default_raid == RaidMode::RAID1) {
    // spare some free zones for dynamic allocation
    for (idx_t idx = AQUAFS_META_ZONES; idx < nr_zones_ / 3; idx++) {
      auto status = allocator.createMappingTwice(idx);
      allocator.setMappingMode(idx, target_default_raid);
      if (!status.ok()) {
        Error(logger_, "Failed to create mapping for zone %x", idx);
      }
    }
  } else {
    assert(false);
  }
  flush_zone_info();
  return s;
}

void RaidAutoZonedBlockDevice::syncBackendInfo() {
  AbstractRaidZonedBlockDevice::syncBackendInfo();
  zone_sz_ *= nr_dev();
  // Debug(logger_, "syncBackendInfo(): blksz=%x, zone_sz=%lx, nr_zones=%x",
  //       block_sz_, zone_sz_, nr_zones_);
}

std::unique_ptr<ZoneList> RaidAutoZonedBlockDevice::ListZones() {
  // Debug(logger_, "ListZones()");
  auto data = new raid_zone_t[nr_zones_];
  memcpy(data, a_zones_.get(), sizeof(raid_zone_t) * nr_zones_);
  return std::make_unique<ZoneList>(data, nr_zones_);
}

IOStatus RaidAutoZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                         uint64_t *max_capacity) {
  Info(logger_, "Reset(start=%lx)", start);
  assert(start % GetZoneSize() == 0);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto mm = allocator.device_zone_map_[i + zone_idx * nr_dev()];
    for (auto &&m : mm) {
      r = devices_[m.device_idx]->Reset(m.zone_idx * def_dev()->GetZoneSize(),
                                        offline, max_capacity);
      Info(logger_, "RAID-A: do reset for device %d, zone %d", m.device_idx,
           m.zone_idx);
      if (!r.ok())
        return r;
      else
        *max_capacity *= nr_dev();
    }
  }
  flush_zone_info();
  // zone_info(zone_idx)->wp = zone_info(zone_idx)->start;
  return r;
}

IOStatus RaidAutoZonedBlockDevice::Finish(uint64_t start) {
  Info(logger_, "Finish(%lx)", start);
  assert(start % GetZoneSize() == 0);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto mm = allocator.device_zone_map_[i + zone_idx * nr_dev()];
    for (auto &&m : mm) {
      r = devices_[m.device_idx]->Finish(m.zone_idx * def_dev()->GetZoneSize());
      Info(logger_, "RAID-A: do finish for device %d, zone %d", m.device_idx,
           m.zone_idx);
      if (!r.ok()) return r;
    }
  }
  // flush_zone_info();
  // FIXME: right?
  zone_info(zone_idx)->wp =
      zone_info(zone_idx)->start + zone_info(zone_idx)->len;
  return r;
}

IOStatus RaidAutoZonedBlockDevice::Close(uint64_t start) {
  Info(logger_, "Close(start=%lx)", start);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto sub_idx = i + zone_idx * nr_dev();
    auto fm = allocator.device_zone_map_.find(sub_idx);
    if (fm == allocator.device_zone_map_.end()) {
      Warn(logger_,
           "Ignoring raid sub zone %lx: not mapping in device zone map",
           sub_idx);
      continue;
    }
    auto mm = allocator.device_zone_map_[sub_idx];
    auto f = allocator.mode_map_.find(sub_idx);
    if (f == allocator.mode_map_.end()) {
      Warn(logger_, "Ignoring raid sub zone %lx: not mapping in raid mode map",
           sub_idx);
      continue;
    }
    Info(logger_,
         "Closing raid sub zone %lx, with %zu device zones, mode=raid%s",
         sub_idx, mm.size(),
         f == allocator.mode_map_.end() ? "?" : raid_mode_str(f->second.mode));
    for (auto &&m : mm) {
      r = devices_[m.device_idx]->Close(m.zone_idx * def_dev()->GetZoneSize());
      if (!r.ok()) {
        Error(logger_, "RAID-A: do close failed for device %d, zone %d! %s",
              m.device_idx, m.zone_idx, r.getState());
        flush_zone_info();
        return r;
      } else {
        Info(logger_, "RAID-A: do close for device %d, zone %d", m.device_idx,
             m.zone_idx);
      }
    }
  }
  // flush_zone_info();
  zone_info(zone_idx)->cond = ZBD_ZONE_COND_CLOSED;
  // return r;
  return IOStatus::OK();
}

int RaidAutoZonedBlockDevice::Read(char *buf, int size, uint64_t pos,
                                   bool direct) {
  // Debug(logger_, "Read(sz=%x, pos=%lx, direct=%s)", size, pos,
  //       std::to_string(direct).c_str());
  if (static_cast<decltype(zone_sz_)>(size) > zone_sz_) {
    // may cross raid zone, split read range as zones
    int sz_read = 0;
    int r;
    while (size > 0) {
      auto req_size =
          std::min(size, static_cast<int>(zone_sz_ - pos % zone_sz_));
      r = Read(buf, req_size, pos, direct);
      if (r > 0) {
        buf += r;
        pos += r;
        sz_read += r;
        size -= r;
      } else {
        return r;
      }
    }
    // flush_zone_info();
    return sz_read;
  } else {
    assert(static_cast<decltype(zone_sz_)>(size) <= zone_sz_);
    auto mode_item = allocator.mode_map_[pos / zone_sz_];
    if (mode_item.mode == RaidMode::RAID_C ||
        // mode_item.mode == RaidMode::RAID1 ||
        mode_item.mode == RaidMode::RAID_NONE) {
      auto m = getAutoDeviceZone(pos);
      auto mapped_pos = getAutoMappedDevicePos(pos);
      auto r = devices_[m.device_idx]->Read(buf, size, mapped_pos, direct);
      // Info(logger_,
      //      "RAID-A: READ raid%s mapping pos=%lx to mapped_pos=%lx, dev=%x,"
      //      "zone=%x; r=%x",
      //      raid_mode_str(mode_item.mode), pos, mapped_pos, m.device_idx,
      //      m.zone_idx, r);
      // if (size < 0x20) {
      //   printf("[[READ]] pos=%lx, size=%x, data:\n", pos, size);
      //   for (int i = 0; i < size; i++) {
      //     printf("%02x ", buf[i] & 0xff);
      //   }
      //   printf("\n");
      // }
      return r;
    } else if (mode_item.mode == RaidMode::RAID1) {
      idx_t raid_zone_idx = pos / zone_sz_;
      // auto raid_zone_offset = pos - raid_zone_idx * zone_sz_;
      idx_t inner_zone_idx_offset = (pos / def_dev()->GetZoneSize()) % nr_dev();
      auto inner_zone_offset = pos % def_dev()->GetZoneSize();
      auto &m = allocator.device_zone_map_[raid_zone_idx * nr_dev() +
                                           inner_zone_idx_offset];
      assert(size <= static_cast<decltype(size)>(def_dev()->GetZoneSize()));
      int r = -1;
      for (auto &mm : m) {
        // TODO: spare read to all devices
        r = devices_[mm.device_idx]->Read(
            buf, size,
            mm.zone_idx * def_dev()->GetZoneSize() + inner_zone_offset, direct);
        // FIXME
        // if (r < 0)
        break;
      }
      if (r < 0) {
        auto status = ScanAndHandleOffline();
        if (status.ok()) {
          // retry this read
          return Read(buf, size, pos, direct);
        } else {
          Error(logger_, "failed to restore data: %s", status.getState());
          return r;
        }
      }
      return r;
    } else if (mode_item.mode == RaidMode::RAID0) {
#ifndef AQUAFS_RAID_URING
      RaidMapItem m;
      uint64_t mapped_pos;
      // split read range as blocks
      int sz_read = 0;
      // TODO: Read blocks in multi-threads
      int r;
      while (size > 0) {
        m = getAutoDeviceZone(pos);
        mapped_pos = getAutoMappedDevicePos(pos);
        auto req_size = std::min(
            size,
            static_cast<int>(GetBlockSize() - mapped_pos % GetBlockSize()));
        r = devices_[m.device_idx]->Read(buf, req_size, mapped_pos, direct);
        // Info(
        //     logger_,
        //     "RAID-A: [read=%x] READ raid0 mapping pos=%lx to
        //     mapped_pos=%lx, " "dev=%x, zone=%x; r=%x", sz_read, pos,
        //     mapped_pos, m.device_idx, m.zone_idx, r);
        if (r > 0) {
          size -= r;
          sz_read += r;
          buf += r;
          pos += r;
        } else {
          return r;
        }
      }
      // flush_zone_info();
      return sz_read;
#else
      uio::io_service service;
      RaidMapItem m;
      uint64_t mapped_pos;
      // split read range as blocks
      int sz_read = 0;
      using req_item_t = std::tuple<int, char *, uint64_t, off_t>;
      std::vector<req_item_t> requests;
      std::vector<ZbdlibBackend *> bes(nr_dev());
      for (decltype(nr_dev()) i = 0; i < nr_dev(); i++) {
#ifdef ROCKSDB_USE_RTTI
        bes[i] = dynamic_cast<ZbdlibBackend *>(devices_[i].get());
        assert(bes[i] != nullptr);
#else
        bes[i] = (ZbdlibBackend *)(devices_[i].get());
#endif
      }
      while (size > 0) {
        m = getAutoDeviceZone(pos);
        mapped_pos = getAutoMappedDevicePos(pos);
        auto req_size = std::min(
            size,
            static_cast<int>(GetBlockSize() - mapped_pos % GetBlockSize()));
        if (req_size == 0) break;
        auto be = bes[m.device_idx];
        assert(be != nullptr);
        int fd = direct ? be->read_direct_f_ : be->read_f_;
        requests.emplace_back(fd, buf, req_size, mapped_pos);
        size -= req_size;
        sz_read += req_size;
        buf += req_size;
        pos += req_size;
      }
      service.run([&]() -> uio::task<> {
        std::vector<uio::task<int>> futures;
        for (auto &&req : requests) {
          uint8_t flags = 0;
          // read do not need order
          // if (req != *req_list.second.cend()) flags |= IOSQE_IO_LINK;
          futures.emplace_back(service.read(std::get<0>(req), std::get<1>(req),
                                            std::get<2>(req), std::get<3>(req),
                                            flags) |
                               uio::panic_on_err("failed to read!", true));
        }
        for (auto &&fut : futures) co_await fut;
      }());
      // flush_zone_info();
#ifdef AQUAFS_SIM_DELAY
      delay_us(calculate_delay_us(size / requests.size()));
#endif
      return sz_read;
#endif
    } else {
      assert(false);
    }
  }
  return -1;
}

int RaidAutoZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  auto pos_raw = pos;
  auto size_raw = size;
  // Debug(logger_, "Write(size=%x, pos=%lx)", size, pos);
  auto dev_zone_sz = def_dev()->GetZoneSize();
  if (static_cast<decltype(dev_zone_sz)>(size) > dev_zone_sz ||
      (size > 1 && pos / dev_zone_sz != (pos + size - 1) / dev_zone_sz)) {
    // may cross raid zone, split write range as zones
    Warn(logger_, "Write across inner zones! splitting write request");
    int sz_written = 0;
    int r;
    while (size > 0) {
      auto req_size = std::min(
          size, static_cast<uint32_t>(dev_zone_sz - pos % dev_zone_sz));
      r = Write(data, req_size, pos);
      if (r > 0) {
        data += r;
        pos += r;
        sz_written += r;
        size -= r;
      } else {
        return r;
      }
    }
    return sz_written;
  } else {
    assert(static_cast<decltype(dev_zone_sz)>(size) <= dev_zone_sz);
    auto mode_item = allocator.mode_map_[pos / zone_sz_];
    if (mode_item.mode == RaidMode::RAID_C ||
        mode_item.mode == RaidMode::RAID_NONE) {
      auto m = getAutoDeviceZone(pos);
      auto mapped_pos = getAutoMappedDevicePos(pos);
      auto r = devices_[m.device_idx]->Write(data, size, mapped_pos);
      // Info(logger_,
      //      "RAID-A: WRITE raid%s mapping pos=%lx to mapped_pos=%lx, size=%x,
      //      " "dev=%x, zone=%x; r=%x", raid_mode_str(mode_item.mode), pos,
      //      mapped_pos, size, m.device_idx, m.zone_idx, r);
      return r;
    } else if (mode_item.mode == RaidMode::RAID1) {
      // raid_zone_idx: 哪一个 RaidZone = 偏移量 / RaidZone 大小
      idx_t raid_zone_idx = pos / zone_sz_;
      // inner_zone_idx: 哪一个 Device Zone = 偏移量 / Device Zone 大小
      idx_t inner_zone_idx = pos / def_dev()->GetZoneSize();
      // inner_zone_idx_offset: 在 RaidZone 中是第几个 Device Zone
      idx_t inner_zone_idx_offset = inner_zone_idx % nr_dev();
      // 在 RaidZone 内的偏移量
      auto inner_zone_offset = pos % def_dev()->GetZoneSize();
      // Raid Zone Sub Index: 在 Device Zone Map 中的索引，这个索引值 / 设备数 = RaidZone Index
      // Raid Zone Sub Index = (RaidZone Index * 设备数量) + (在 RaidZone 中是第几个 Device Zone)
      auto sub_idx = raid_zone_idx * nr_dev() + inner_zone_idx_offset;
      // 用 Raid Zone Sub Index 查找映射信息
      auto fm = allocator.device_zone_map_.find(sub_idx);
      if (fm == allocator.device_zone_map_.end()) {
        Error(logger_,
              "Cannot locate raid1 write: sub idx %zx not in device zone map",
              sub_idx);
      }
      auto m = fm->second;
      assert(size <= static_cast<decltype(size)>(def_dev()->GetZoneSize()));
      // write to all mapped zones
      int r;
      // std::string mp_info = "[mp] raid zone " + std::to_string(sub_idx) + ":
      // "; for (auto &mm : m)
      //   mp_info += "dev " + std::to_string(mm.device_idx) + ", zone " +
      //              std::to_string(mm.zone_idx) + "; ";
      for (auto mm : m) {
        r = devices_[mm.device_idx]->Write(
            data, size,
            mm.zone_idx * def_dev()->GetZoneSize() + inner_zone_offset);
        // Info(logger_,
        //      "writing raid1: pos=%lx, size=%x, backend dev=%x, zone=%x, r=%x,
        //      " "mp: %s", pos, size, mm.device_idx, mm.zone_idx, r,
        //      mp_info.c_str());
        if (r < 0) {
          Error(logger_,
                "Cannot write raid1! r=%d, pos=%lx, size=%x, backend dev=%x, "
                "zone=%x, writing dev pos %lx",
                r, pos, size, mm.device_idx, mm.zone_idx,
                mm.zone_idx * def_dev()->GetZoneSize() + inner_zone_offset);
          return r;
        }
      }
      return r;
    } else if (mode_item.mode == RaidMode::RAID0) {
#ifndef AQUAFS_RAID_URING
      RaidMapItem m;
      uint64_t mapped_pos;
      // split write range as blocks
      int sz_written = 0;
      // TODO: Write blocks in multi-threads
      int r;
      while (size > 0) {
        m = getAutoDeviceZone(pos);
        mapped_pos = getAutoMappedDevicePos(pos);
        // auto z = devices_[m.device_idx]->ListZones();
        // auto p = reinterpret_cast<raid_zone_t *>(z->GetData());
        // auto pp = p[m.zone_idx];
        // if ((pos == 0x20000000 || mapped_pos == 0x8000000) &&
        //     (m.device_idx == 0 && m.zone_idx == 4)) {
        //   mapped_pos = getAutoMappedDevicePos(pos);
        //   Info(logger_,
        //        "[DBG] RAID-A-0: dev_zone_info: st=%llx, cap=%llx, "
        //        "wp=%llx, sz=%llx; to write: dev=%x, zone=%x, pos=%lx,sz=%x",
        //        pp.start, pp.capacity, pp.wp, pp.capacity, m.device_idx,
        //        m.zone_idx, mapped_pos, size);
        // }
        auto req_size =
            std::min(size, static_cast<uint32_t>(GetBlockSize() -
                                                 mapped_pos % GetBlockSize()));
        r = devices_[m.device_idx]->Write(data, req_size, mapped_pos);
        // Info(logger_,
        //      "RAID-A: [written=%x] WRITE raid0 mapping pos=%lx to "
        //      "mapped_pos=%lx, "
        //      "dev=%x, zone=%x; r=%x",
        //      sz_written, pos, mapped_pos, m.device_idx, m.zone_idx, r);
        if (r > 0) {
          size -= r;
          sz_written += r;
          data += r;
          pos += r;
        } else {
          return r;
        }
      }
      // flush_zone_info();
      zone_info(pos_raw / zone_sz_)->wp += size;
      return sz_written;
#else
      uio::io_service service;
      RaidMapItem m;
      uint64_t mapped_pos;
      uint32_t sz_written = 0;
      using req_item_t = std::tuple<char *, uint64_t, off_t>;
      // <dev, zone> -> vec<ordered req>
      std::map<std::pair<int, idx_t>, std::vector<req_item_t>> requests;
      std::vector<ZbdlibBackend *> bes(nr_dev());
      for (decltype(nr_dev()) i = 0; i < nr_dev(); i++) {
#ifdef ROCKSDB_USE_RTTI
        bes[i] = dynamic_cast<ZbdlibBackend *>(devices_[i].get());
        assert(bes[i] != nullptr);
#else
        bes[i] = (ZbdlibBackend *)(devices_[i].get());
#endif
      }
      while (size > 0) {
        m = getAutoDeviceZone(pos);
        mapped_pos = getAutoMappedDevicePos(pos);
        auto req_size =
            std::min(size, static_cast<uint32_t>(GetBlockSize() -
                                                 mapped_pos % GetBlockSize()));
        auto dev_zone_idx = mapped_pos / def_dev()->GetZoneSize();
        if (req_size == 0) break;
        auto be = bes[m.device_idx];
        assert(be != nullptr);
        int fd = be->write_f_;
        requests[{fd, dev_zone_idx}].emplace_back(data, req_size, mapped_pos);
        size -= req_size;
        sz_written += req_size;
        data += req_size;
        pos += req_size;
      }
      service.run([&]() -> uio::task<> {
        // std::vector<uio::task<int>> futures;
        std::vector<uio::sqe_awaitable> futures;
        for (const auto &req_list : requests) {
          for (auto &&req : req_list.second) {
            uint8_t flags = 0;
            if (req != *req_list.second.cend()) flags |= IOSQE_IO_LINK;
            futures.emplace_back(
                service.write(req_list.first.first, std::get<0>(req),
                              std::get<1>(req), std::get<2>(req), flags));
            // pwrite(req_list.first.first, std::get<0>(req), std::get<1>(req),
            //        std::get<2>(req));
          }
        }
        // for (auto &&fut : futures) {
        //   // delay_us(0);
        //   // co_await fut | uio::panic_on_err("failed to write!", true);
        //   // co_await fut;
        //   break;
        // }
        co_return;
      }());
#ifdef AQUAFS_SIM_DELAY
      delay_us(calculate_delay_us(size / requests.size()));
#endif
      // flush_zone_info();
      zone_info(pos_raw / zone_sz_)->wp += size_raw;
      return static_cast<int>(sz_written);
#endif
    }
  }
  return -1;
}

int RaidAutoZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  // Debug(logger_, "InvalidateCache(pos=%lx, sz=%lx)", pos, size);
  assert(size % zone_sz_ == 0);
  if (static_cast<decltype(zone_sz_)>(size) > zone_sz_) {
    // may cross raid zone, split range as zones
    int r;
    while (size > 0) {
      auto req_size = std::min(
          size, static_cast<decltype(size)>(zone_sz_ - pos % zone_sz_));
      r = InvalidateCache(pos, req_size);
      if (!r) {
        pos += zone_sz_;
        size -= zone_sz_;
      } else {
        return r;
      }
    }
    return 0;
  } else {
    assert(pos % GetZoneSize() == 0);
    assert(static_cast<decltype(zone_sz_)>(size) <= zone_sz_);
    auto m = getAutoDeviceZone(pos);
    auto mapped_pos = getAutoMappedDevicePos(pos);
    auto r = devices_[m.device_idx]->InvalidateCache(mapped_pos, size);
    flush_zone_info();
    return r;
  }
}

bool RaidAutoZonedBlockDevice::ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                                         idx_t idx) {
  // Info(logger_, "ZoneIsSwr(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsSwr(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                             idx_t idx) {
  // Info(logger_, "ZoneIsOffline(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsOffline(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                                              idx_t idx) {
  // Debug(logger_, "ZoneIsWriteable(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsWritable(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                                            idx_t idx) {
  // Info(logger_, "ZoneIsActive(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsActive(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  // Info(logger_, "ZoneIsOpen(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsOpen(z, m.zone_idx);
}

uint64_t RaidAutoZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                             idx_t idx) {
  // flush_zone_info();
  // Debug(logger_, "ZoneStart(idx=%x)", idx);
  // FIXME?
  return reinterpret_cast<raid_zone_t *>(zones.get()->GetData())[idx].start;
}

uint64_t RaidAutoZonedBlockDevice::ZoneMaxCapacity(
    std::unique_ptr<ZoneList> &zones, idx_t idx) {
  // flush_zone_info();
  // Debug(logger_, "ZoneMaxCapacity(idx=%x)", idx);
  // FIXME: capacity == max_capacity ?
  return reinterpret_cast<raid_zone_t *>(zones.get()->GetData())[idx].capacity;
}

uint64_t RaidAutoZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  // Debug(logger_, "ZoneWp(idx=%x)", idx);
  // flush_zone_info();
  auto r = reinterpret_cast<raid_zone_t *>(zones.get()->GetData())[idx].wp;
  // Info(logger_, "RAID-A: ZoneWp=%llx", r);
  return r;
}

void RaidAutoZonedBlockDevice::flush_zone_info() {
  // TODO
  // std::vector<std::unique_ptr<ZoneList>> dev_zone_list(nr_dev());
  // for (idx_t idx = 0; idx < nr_dev(); idx++) {
  //   auto z = devices_[idx]->ListZones();
  //   dev_zone_list[idx] = std::move(z);
  // }
  for (idx_t idx = 0; idx < nr_zones_; idx++) {
    auto found_mode_map = allocator.mode_map_.find(idx);
    if (found_mode_map == allocator.mode_map_.end()) continue;
    auto mode_item = found_mode_map->second;
    // Debug(logger_, "flush_zone_info: zone %x is raid%s", idx,
    //      raid_mode_str(mode_item.mode));
    std::unique_ptr<ZoneList> zone_list = nullptr;
    auto p = a_zones_.get();
    p[idx].start = idx * zone_sz_;
    if (mode_item.mode == RaidMode::RAID_NONE ||
        mode_item.mode == RaidMode::RAID0 ||
        mode_item.mode == RaidMode::RAID_C) {
      std::vector<RaidMapItem> map_items(nr_dev());
      for (idx_t i = 0; i < nr_dev(); i++)
        map_items[i] =
            getAutoDeviceZone(idx * zone_sz_ + i * def_dev()->GetZoneSize());
      auto map_item = map_items.front();
      zone_list = devices_[map_item.device_idx]->ListZones();
      uint64_t wp = std::accumulate(
          map_items.begin(), map_items.end(), static_cast<uint64_t>(0),
          [&](uint64_t sum, auto &item) {
            auto z = devices_[item.device_idx]->ListZones();
            auto s = devices_[item.device_idx]->ZoneStart(z, item.zone_idx);
            auto w = devices_[item.device_idx]->ZoneWp(z, item.zone_idx);
            // printf("\tdev[%x][%x] st=%lx, wp=%lx\n", item.device_idx,
            //        item.zone_idx, s, w);
            assert(w >= s);
            return sum + (w - s);
          });
      wp += p[idx].start;
      // printf("[%x] total wp=%lx\n", idx, wp);
      p[idx].wp = wp;
    } else if (mode_item.mode == RaidMode::RAID1) {
      uint64_t cnt = 0;
      for (idx_t offset = 0; offset < nr_dev(); offset++) {
        auto sub_idx = idx * nr_dev() + offset;
        auto fm = allocator.device_zone_map_.find(sub_idx);
        if (fm == allocator.device_zone_map_.end()) {
          Error(logger_,
                "flush_zone_info raid1: failed to locate sub idx %zx, ignore",
                sub_idx);
          continue;
        }
        auto m = fm->second;
        auto mm = m[0];
        zone_list = devices_[mm.device_idx]->ListZones();
        auto c = devices_[mm.device_idx]->ZoneWp(zone_list, mm.zone_idx) -
                 devices_[mm.device_idx]->ZoneStart(zone_list, mm.zone_idx);
        // if (c > 0) {
        //   Info(logger_, "adding size %lx in dev %x zone %x to raid zone %x",
        //   c,
        //        mm.device_idx, mm.zone_idx, idx);
        // }
        cnt += c;
      }
      p[idx].wp = p[idx].start + cnt;
    }
    raid_zone_t *zone_list_ptr =
        reinterpret_cast<raid_zone_t *>(zone_list->GetData());
    // FIXME: ZoneFS
    p[idx].flags = zone_list_ptr->flags;
    p[idx].type = zone_list_ptr->type;
    p[idx].cond = zone_list_ptr->cond;
    memcpy(p[idx].reserved, zone_list_ptr->reserved, sizeof(p[idx].reserved));
    // p[idx].capacity = devices_[map_item.device_idx]->ZoneMaxCapacity(
    //                       zone_list, map_item.zone_idx) *
    //                   nr_dev();
    p[idx].capacity = zone_sz_;
    p[idx].len = p[idx].capacity;
  }
}
void RaidAutoZonedBlockDevice::layout_update(
    RaidAutoZonedBlockDevice::device_zone_map_t &&device_zone,
    RaidAutoZonedBlockDevice::mode_map_t &&mode_map) {
  Warn(logger_, "layout_update! device_zone %zu items, mode_map %zu items",
       device_zone.size(), mode_map.size());
  for (auto &&p : device_zone) allocator.device_zone_map_.insert(p);
  for (auto &&p : mode_map) allocator.mode_map_.insert(p);
  flush_zone_info();
}
void RaidAutoZonedBlockDevice::layout_setup(
    RaidAutoZonedBlockDevice::device_zone_map_t &&device_zone,
    RaidAutoZonedBlockDevice::mode_map_t &&mode_map) {
  allocator.device_zone_map_ = std::move(device_zone);
  allocator.mode_map_ = std::move(mode_map);
  flush_zone_info();
}
template <class T>
RaidMapItem RaidAutoZonedBlockDevice::getAutoDeviceZoneFromIdx(T idx) {
  auto p = allocator.device_zone_map_.find(idx * nr_dev());
  if (p == allocator.device_zone_map_.end()) return {};
  if (p->second.size() > 0)
    return p->second[0];
  else {
    Error(logger_, "failed to get idx %x! fall back to default 0", idx);
    return {};
  }
}
template <class T>
T RaidAutoZonedBlockDevice::getAutoMappedDevicePos(T pos) {
  // 第几个 RaidZone (raid_zone_idx) = 偏移量 / RaidZone 大小
  auto raid_zone_idx = pos / zone_sz_;
  // 找到这个偏移量对应着的 RaidZone 映射数据，映射到哪一个 Device 上的哪一个 Zone
  RaidMapItem map_item = getAutoDeviceZone(pos);
  // 找这个 RaidZone Index 对应的 RaidMode，即 0/1/c
  auto mode_item = allocator.mode_map_[raid_zone_idx];
  // 这个偏移量是逻辑上第几个 Block (Block Index) = 偏移量 / Block 大小
  auto blk_idx = pos / block_sz_;
  // if (mode_item.mode == RaidMode::RAID_NONE) {
  //   return pos;
  // } else
  if (mode_item.mode == RaidMode::RAID0) {
    // RAID0 逻辑：
    // Device 上对应 Zone 的偏移（start或者说base）
    auto base = map_item.zone_idx * def_dev()->GetZoneSize();
    // 一个 RaidZone 有多少 Block (nr_blk_in_raid_zone) = RaidZone 大小 / Block 大小
    auto nr_blk_in_raid_zone = zone_sz_ / block_sz_;
    // blk_idx_raid_zone: Block Index 在一个 RaidZone 内的偏移量
    //   = Block Index % 一个 RaidZone 有多少 Block
    auto blk_idx_raid_zone = blk_idx % nr_blk_in_raid_zone;
    // blk_idx_dev_zone: Block Index 在一个 Device Zone 内的偏移量
    //   = Block Index 在一个 RaidZone 内的偏移量 / Device 数量
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
template <class T>
RaidMapItem RaidAutoZonedBlockDevice::getAutoDeviceZone(T pos) {
  return allocator.device_zone_map_[getAutoDeviceZoneIdx(pos)][0];
}
template <class T>
idx_t RaidAutoZonedBlockDevice::getAutoDeviceZoneIdx(T pos) {
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
    return raid_zone_idx * nr_dev() + raid_zone_inner_idx;
  } else if (mode_item.mode == RaidMode::RAID0) {
    // Info(logger_, "\t[pos=%x] raid_zone_idx=%lx raid_zone_block_idx = %lx",
    //      static_cast<uint32_t>(pos), raid_zone_idx, raid_zone_block_idx);
    return raid_zone_idx * nr_dev() + raid_zone_block_idx % nr_dev();
  }
  Warn(logger_, "Cannot locate device zone at pos=%x",
       static_cast<uint32_t>(pos));
  return {};
}
Status RaidMapItem::DecodeFrom(Slice *input) {
  GetFixed32(input, &device_idx);
  GetFixed32(input, &zone_idx);
  GetFixed16(input, &invalid);
  return Status::OK();
}
Status RaidModeItem::DecodeFrom(Slice *input) {
  GetFixed32(input, reinterpret_cast<uint32_t *>(&mode));
  GetFixed32(input, &option);
  return Status::OK();
}

Status RaidAutoZonedBlockDevice::ScanAndHandleOffline() {
  idx_t handle_device = 0;
  idx_t handle_zone_sub = 0;
  idx_t handle_device_zone = 0;
  bool will_handle = false;
  for (auto &p : allocator.device_zone_map_) {
    for (auto &&m : p.second) {
      auto d = m.device_idx;
      auto z = m.zone_idx;
      auto zones = devices_[d]->ListZones();
      if (devices_[d]->ZoneIsOffline(zones, z)) {
        will_handle = true;
        handle_device = d;
        handle_device_zone = z;
        handle_zone_sub = p.first;
        Warn(logger_, "found offline zone: dev %x zone %x, raid zone sub %x", d,
             z, p.first);
        break;
      }
    }
    if (will_handle) break;
  }
  if (will_handle) {
    auto mode = allocator.mode_map_[handle_zone_sub / nr_dev()].mode;
    auto &mp = allocator.device_zone_map_[handle_zone_sub];
    auto can_recover = mode == RaidMode::RAID1;
    if (can_recover) {
      // mark as offline
      allocator.setOffline(handle_device, handle_device_zone);
      if (mode == RaidMode::RAID1) {
        std::string mp_info = "[mp-before] raid zone %x: ";
        for (auto &m : mp)
          mp_info += "dev " + std::to_string(m.device_idx) + ", zone " +
                     std::to_string(m.zone_idx) + "; ";
        Info(logger_, "this mp before handle: %s", mp_info.c_str());
        // remove old mapping
        // in c++2a
        // std::erase_if(allocator.device_zone_map_[handle_zone_sub], );
        auto f =
            std::find_if(mp.begin(), mp.end(), [&](const RaidMapItem &item) {
              return item.device_idx == handle_device &&
                     item.zone_idx == handle_device_zone;
            });
        if (f != mp.end()) {
          Warn(logger_, "remove mapping for dev %x zone %x", f->device_idx,
               f->zone_idx);
          mp.erase(f);
        }
      }
      // allocate new
      idx_t dev_zone_new = -1;
      auto status = allocator.createOneMappingAt(handle_zone_sub, handle_device,
                                                 dev_zone_new);
      if (!status.ok()) {
        Error(logger_,
              "Zone sub %x offline (dev %x, dev zone %x), and recover data "
              "failed: %s",
              handle_zone_sub, handle_device, handle_device_zone,
              status.getState());
      }
      if (mode == RaidMode::RAID1) {
        std::string mp_info =
            "[mp] raid zone " + std::to_string(handle_zone_sub) + ": ";
        for (auto &m : mp)
          mp_info += "dev " + std::to_string(m.device_idx) + ", zone " +
                     std::to_string(m.zone_idx) + "; ";
        Info(logger_, "this mp: %s", mp_info.c_str());
        // clone data
        auto restoring =
            std::find_if(mp.begin(), mp.end(), [&](const RaidMapItem &item) {
              return item.device_idx == handle_device &&
                     item.zone_idx == dev_zone_new;
            });
        assert(restoring != mp.end());
        auto fine = std::find_if(
            mp.begin(), mp.end(),
            [&](const RaidMapItem &item) { return item != *restoring; });
        auto zones = devices_[fine->device_idx]->ListZones();
        auto wp = devices_[fine->device_idx]->ZoneWp(zones, fine->zone_idx);
        auto start =
            devices_[fine->device_idx]->ZoneStart(zones, fine->zone_idx);
        Info(logger_, "fine zone: dev=%x, zone=%x, wp=%lx, start=%lx",
             fine->device_idx, fine->zone_idx, wp, start);
        assert(wp >= start);
        auto sz = wp - start;
        char *buf = nullptr;
        if (posix_memalign((void **)(&buf), getpagesize(), sz)) {
          return Status::IOError("Allocate memory failed!");
        }
        auto read_sz = devices_[fine->device_idx]->Read(
            buf, static_cast<int>(sz),
            restoring->zone_idx * zone_sz_ / nr_dev(), false);
        if (read_sz < 0) {
          Error(logger_, "Cannot read data from dev %x zone %x, sz=%lx",
                fine->device_idx, fine->zone_idx, sz);
          free(buf);
          return Status::IOError("Cannot recover data");
        }
        // restore data
        bool tmp_offline = false;
        uint64_t tmp_max_capacity = 0;
        devices_[restoring->device_idx]->Reset(
            restoring->zone_idx * zone_sz_ / nr_dev(), &tmp_offline,
            &tmp_max_capacity);
        auto write_start = restoring->zone_idx * def_dev()->GetZoneSize();
        auto write_dev = restoring->device_idx;
        Info(logger_, "restoring data to dev %x zone %x, sz=%lx, pos=%lx",
             write_dev, restoring->zone_idx, sz, write_start);
        uint32_t tmp_2;
        uint32_t tmp_3;
        status = devices_[write_dev]->Open(false, false, &tmp_2, &tmp_3);
        assert(status.ok());
        status = devices_[write_dev]->Reset(write_start, &tmp_offline,
                                            &tmp_max_capacity);
        assert(status.ok());
        auto written = devices_[write_dev]->Write(buf, sz, write_start);
        if (static_cast<decltype(sz)>(written) != sz) {
          Error(logger_, "Cannot write restored data! written=%x, cause: %s",
                written, strerror(errno));
          free(buf);
          return Status::IOError("Cannot recover data");
        } else {
          free(buf);
        }
      }
    } else {
      Error(
          logger_,
          "Zone sub %x offline (dev %x, dev zone %x), and cannot recover data!",
          handle_zone_sub, handle_device, handle_device_zone);
      return Status::IOError("Cannot recover data");
    }
  }
  return Status::OK();
}
void RaidAutoZonedBlockDevice::setZoneOffline(unsigned int idx,
                                              unsigned int idx2, bool offline) {
  if (offline) Warn(logger_, "setting dev %x zone %x to offline!", idx, idx2);
  devices_[idx]->setZoneOffline(idx2, 0, offline);
}

}  // namespace AQUAFS_NAMESPACE