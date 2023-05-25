// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>


#include "io_aquafs.h"

#include "zbd_aquafs.h"
namespace aquafs {

// Indicate what stats info we want.
class AquaFSSnapshotOptions {
 public:
  // Global zoned device stats info
  bool zbd_ = false;
  // Per zone stats info
  bool zone_ = false;
  // Get all file->extents & extent->file mappings
  bool zone_file_ = false;
  bool trigger_report_ = false;
  bool log_garbage_ = false;
  bool as_lock_free_as_possible_ = true;
};

class ZBDSnapshot {
 public:
  uint64_t free_space;
  uint64_t used_space;
  uint64_t reclaimable_space;

 public:
  ZBDSnapshot() = default;
  // ZBDSnapshot& operator=(const ZBDSnapshot&) = delete;
  // ZBDSnapshot(const ZBDSnapshot&) = default;
  explicit ZBDSnapshot(ZonedBlockDevice& zbd)
      : free_space(zbd.GetFreeSpace()),
        used_space(zbd.GetUsedSpace()),
        reclaimable_space(zbd.GetReclaimableSpace()) {}
};

class ZoneSnapshot {
 public:
  uint64_t start;
  uint64_t wp;

  uint64_t capacity;
  uint64_t used_capacity;
  uint64_t max_capacity;

 public:
  ZoneSnapshot(const Zone& zone)
      : start(zone.start_),
        wp(zone.wp_),
        capacity(zone.capacity_),
        used_capacity(zone.used_capacity_),
        max_capacity(zone.max_capacity_) {}
};

class ZoneExtentSnapshot {
 public:
  uint64_t start;
  uint64_t length;
  uint64_t zone_start;
  std::string filename;

 public:
  ZoneExtentSnapshot(const ZoneExtent& extent, const std::string fname)
      : start(extent.start_),
        length(extent.length_),
        zone_start(extent.zone_->start_),
        filename(fname) {}
};

class ZoneFileSnapshot {
 public:
  uint64_t file_id;
  std::string filename;
  std::vector<ZoneExtentSnapshot> extents;

 public:
  ZoneFileSnapshot(ZoneFile& file)
      : file_id(file.GetID()), filename(file.GetFilename()) {
    for (const auto* extent : file.GetExtents()) {
      extents.emplace_back(*extent, filename);
    }
  }
};

class AquaFSSnapshot {
 public:
  AquaFSSnapshot() = default;

  AquaFSSnapshot& operator=(AquaFSSnapshot&& snapshot) noexcept {
    zbd_ = snapshot.zbd_;
    zones_ = std::move(snapshot.zones_);
    zone_files_ = std::move(snapshot.zone_files_);
    extents_ = std::move(snapshot.extents_);
    return *this;
  }

 public:
  ZBDSnapshot zbd_{};
  std::vector<ZoneSnapshot> zones_;
  std::vector<ZoneFileSnapshot> zone_files_;
  std::vector<ZoneExtentSnapshot> extents_;
};

}  // namespace aquafs
