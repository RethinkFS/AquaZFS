//
// Created by chiro on 23-5-6.
//

#include "zone_raid_auto.h"

#include <memory>
#include <queue>
#include <utility>

#include "base/io_status.h"
#include "base/coding.h"

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
      device_zone_map_[idx * nr_dev() + i] = {
          0, static_cast<idx_t>(idx * nr_dev() + i), 0};
    mode_map_[idx] = {RaidMode::RAID_NONE, 0};
  }
  syncBackendInfo();
}

IOStatus RaidAutoZonedBlockDevice::Open(bool readonly, bool exclusive,
                                        unsigned int *max_active_zones,
                                        unsigned int *max_open_zones) {
  auto s = AbstractRaidZonedBlockDevice::Open(readonly, exclusive,
                                              max_active_zones, max_open_zones);
  // allocate default layout
  a_zones_.reset(new raid_zone_t[nr_zones_]);
  memset(a_zones_.get(), 0, sizeof(raid_zone_t) * nr_zones_);
  std::queue<size_t> available_devices;
  std::vector<std::queue<idx_t>> available_zones(nr_dev());
  for (size_t i = 0; i < nr_dev(); i++) {
    available_devices.push(i);
    for (idx_t idx = (i ? 0 : (AQUAFS_META_ZONES * nr_dev()));
         idx < def_dev()->GetNrZones(); idx++)
      available_zones[i].push(idx);
  }
  for (idx_t idx = AQUAFS_META_ZONES; idx < nr_zones_; idx++) {
    for (size_t i = 0; i < nr_dev(); i++) {
      // FIXME: not enough zones?
      if (available_devices.empty()) break;
      idx_t d = available_devices.front();
      auto d_next = (d == nr_dev() - 1) ? 0 : d + 1;
      available_devices.pop();
      idx_t ti;
      if (available_zones[d].empty()) {
        if (available_zones[d_next].empty()) {
          // FIXME
          Info(logger_,
               "available_zones[d_next=%d] empty! Cannot allocate for "
               "device_zone_map_[%lx, idx=%x, i=%zx]",
               d_next, idx * nr_dev() + i, idx, i);
          break;
        }
        assert(!available_zones[d_next].empty());
        ti = available_zones[d_next].front();
        available_zones[d_next].pop();
      } else {
        assert(!available_zones[d].empty());
        ti = available_zones[d].front();
        available_zones[d].pop();
        available_devices.push(d_next);
      }
      device_zone_map_[idx * nr_dev() + i] = {d, ti, 0};
      // Info(logger_,
      //      "RAID-A: pre-allocate raid zone %x device_zone_map_[(idx*nr_dev
      //      + " "i)=%zx] = "
      //      "{d=%x, ti=%x, 0}",
      //      idx, idx * nr_dev() + i, d, ti);
    }
    mode_map_[idx] = {RaidMode::RAID0, 0};
    // mode_map_[idx] = {RaidMode::RAID1, 0};
    // mode_map_[idx] = {RaidMode::RAID_C, 0};
    // mode_map_[idx] = {RaidMode::RAID_NONE, 0};
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
  return {};
}

IOStatus RaidAutoZonedBlockDevice::Reset(uint64_t start, bool *offline,
                                         uint64_t *max_capacity) {
  Info(logger_, "Reset(start=%lx)", start);
  assert(start % GetZoneSize() == 0);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto m = device_zone_map_[i + zone_idx * nr_dev()];
    r = devices_[m.device_idx]->Reset(m.zone_idx * def_dev()->GetZoneSize(),
                                      offline, max_capacity);
    Info(logger_, "RAID-A: do reset for device %d, zone %d", m.device_idx,
         m.zone_idx);
    if (!r.ok())
      return r;
    else
      *max_capacity *= nr_dev();
  }
  flush_zone_info();
  return r;
}

IOStatus RaidAutoZonedBlockDevice::Finish(uint64_t start) {
  Info(logger_, "Finish(%lx)", start);
  assert(start % GetZoneSize() == 0);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto m = device_zone_map_[i + zone_idx * nr_dev()];
    r = devices_[m.device_idx]->Finish(m.zone_idx * def_dev()->GetZoneSize());
    Info(logger_, "RAID-A: do finish for device %d, zone %d", m.device_idx,
         m.zone_idx);
    if (!r.ok()) return r;
  }
  flush_zone_info();
  return r;
}

IOStatus RaidAutoZonedBlockDevice::Close(uint64_t start) {
  Info(logger_, "Close(start=%lx)", start);
  IOStatus r{};
  auto zone_idx = start / zone_sz_;
  for (size_t i = 0; i < nr_dev(); i++) {
    auto m = device_zone_map_[i + zone_idx * nr_dev()];
    r = devices_[m.device_idx]->Close(m.zone_idx * def_dev()->GetZoneSize());
    Info(logger_, "RAID-A: do close for device %d, zone %d", m.device_idx,
         m.zone_idx);
    if (!r.ok()) return r;
  }
  flush_zone_info();
  return r;
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
    flush_zone_info();
    return sz_read;
  } else {
    assert(static_cast<decltype(zone_sz_)>(size) <= zone_sz_);
    auto mode_item = mode_map_[pos / zone_sz_];
    auto m = getAutoDeviceZone(pos);
    auto mapped_pos = getAutoMappedDevicePos(pos);
    if (mode_item.mode == RaidMode::RAID_C ||
        mode_item.mode == RaidMode::RAID1 ||
        mode_item.mode == RaidMode::RAID_NONE) {
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
    } else if (mode_item.mode == RaidMode::RAID0) {
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
      flush_zone_info();
      return sz_read;
    } else {
      assert(false);
    }
  }
}

int RaidAutoZonedBlockDevice::Write(char *data, uint32_t size, uint64_t pos) {
  // Debug(logger_, "Write(size=%x, pos=%lx)", size, pos);
  auto dev_zone_sz = def_dev()->GetZoneSize();
  if (static_cast<decltype(dev_zone_sz)>(size) > dev_zone_sz) {
    // may cross raid zone, split write range as zones
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
    auto mode_item = mode_map_[pos / zone_sz_];
    auto m = getAutoDeviceZone(pos);
    auto mapped_pos = getAutoMappedDevicePos(pos);
    if (mode_item.mode == RaidMode::RAID_C ||
        mode_item.mode == RaidMode::RAID1 ||
        mode_item.mode == RaidMode::RAID_NONE) {
      auto r = devices_[m.device_idx]->Write(data, size, mapped_pos);
      // Info(logger_,
      //      "RAID-A: WRITE raid%s mapping pos=%lx to mapped_pos=%lx, size=%x, "
      //      "dev=%x, zone=%x; r=%x",
      //      raid_mode_str(mode_item.mode), pos, mapped_pos, size, m.device_idx,
      //      m.zone_idx, r);
      return r;
    } else if (mode_item.mode == RaidMode::RAID0) {
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
      flush_zone_info();
      return sz_written;
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
  // Debug(logger_, "ZoneIsSwr(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsSwr(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                                             idx_t idx) {
  // Debug(logger_, "ZoneIsOffline(idx=%x)", idx);
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
  // Debug(logger_, "ZoneIsActive(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsActive(z, m.zone_idx);
}

bool RaidAutoZonedBlockDevice::ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  // Debug(logger_, "ZoneIsOpen(idx=%x)", idx);
  auto m = getAutoDeviceZoneFromIdx(idx);
  auto z = devices_[m.device_idx]->ListZones();
  return devices_[m.device_idx]->ZoneIsOpen(z, m.zone_idx);
}

uint64_t RaidAutoZonedBlockDevice::ZoneStart(std::unique_ptr<ZoneList> &zones,
                                             idx_t idx) {
  flush_zone_info();
  // Debug(logger_, "ZoneStart(idx=%x)", idx);
  // FIXME?
  return reinterpret_cast<raid_zone_t *>(zones.get()->GetData())[idx].start;
}

uint64_t RaidAutoZonedBlockDevice::ZoneMaxCapacity(
    std::unique_ptr<ZoneList> &zones, idx_t idx) {
  flush_zone_info();
  // Debug(logger_, "ZoneMaxCapacity(idx=%x)", idx);
  // FIXME: capacity == max_capacity ?
  return reinterpret_cast<raid_zone_t *>(zones.get()->GetData())[idx].capacity;
}

uint64_t RaidAutoZonedBlockDevice::ZoneWp(std::unique_ptr<ZoneList> &zones,
                                          idx_t idx) {
  // Debug(logger_, "ZoneWp(idx=%x)", idx);
  flush_zone_info();
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
    auto mode_item = mode_map_[idx];
    std::vector<RaidMapItem> map_items(nr_dev());
    for (idx_t i = 0; i < nr_dev(); i++)
      map_items[i] =
          getAutoDeviceZone(idx * zone_sz_ + i * def_dev()->GetZoneSize());
    auto map_item = map_items.front();
    auto zone_list = devices_[map_item.device_idx]->ListZones();
    auto zone_list_ptr = reinterpret_cast<raid_zone_t *>(zone_list->GetData());
    auto p = a_zones_.get();
    p[idx].start = idx * zone_sz_;
    if (mode_item.mode == RaidMode::RAID_NONE ||
        mode_item.mode == RaidMode::RAID0 ||
        mode_item.mode == RaidMode::RAID_C) {
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
      p[idx].wp =
          devices_[map_item.device_idx]->ZoneWp(zone_list, map_item.zone_idx);
    }
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
  for (auto &&p: device_zone) device_zone_map_.insert(p);
  for (auto &&p: mode_map) mode_map_.insert(p);
  flush_zone_info();
}

void RaidAutoZonedBlockDevice::layout_setup(
    RaidAutoZonedBlockDevice::device_zone_map_t &&device_zone,
    RaidAutoZonedBlockDevice::mode_map_t &&mode_map) {
  device_zone_map_ = std::move(device_zone);
  mode_map_ = std::move(mode_map);
  flush_zone_info();
}

template<class T>
RaidMapItem RaidAutoZonedBlockDevice::getAutoDeviceZoneFromIdx(T idx) {
  return device_zone_map_[idx * nr_dev()];
}

template<class T>
T RaidAutoZonedBlockDevice::getAutoMappedDevicePos(T pos) {
  auto raid_zone_idx = pos / zone_sz_;
  RaidMapItem map_item = getAutoDeviceZone(pos);
  auto mode_item = mode_map_[raid_zone_idx];
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
  } else {
    return map_item.zone_idx * def_dev()->GetZoneSize() +
           ((blk_idx % (def_dev()->GetZoneSize() / block_sz_)) * block_sz_) +
           pos % block_sz_;
  }
}

template<class T>
RaidMapItem RaidAutoZonedBlockDevice::getAutoDeviceZone(T pos) {
  return device_zone_map_[getAutoDeviceZoneIdx(pos)];
}

template<class T>
idx_t RaidAutoZonedBlockDevice::getAutoDeviceZoneIdx(T pos) {
  auto raid_zone_idx = pos / zone_sz_;
  auto raid_zone_inner_idx =
      (pos - (raid_zone_idx * zone_sz_)) / def_dev()->GetZoneSize();
  auto raid_block_idx = pos / block_sz_;
  // index of block in this raid zone
  auto raid_zone_block_idx =
      raid_block_idx - (raid_zone_idx * (zone_sz_ / block_sz_));
  auto mode_item = mode_map_[raid_zone_idx];
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
}  // namespace aquafs