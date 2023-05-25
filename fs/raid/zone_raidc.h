//
// Created by chiro on 23-5-6.
//

#ifndef ROCKSDB_ZONE_RAIDC_H
#define ROCKSDB_ZONE_RAIDC_H

#include <cstdint>

#include "zone_raid.h"

namespace aquafs {
class RaidCZonedBlockDevice : public AbstractRaidZonedBlockDevice {
 public:
  RaidCZonedBlockDevice(
      const std::shared_ptr<Logger> &logger,
      std::vector<std::unique_ptr<ZonedBlockDeviceBackend>> &&devices);
  std::unique_ptr<ZoneList> ListZones() override;
  IOStatus Reset(uint64_t start, bool *offline,
                 uint64_t *max_capacity) override;
  IOStatus Finish(uint64_t start) override;
  IOStatus Close(uint64_t start) override;
  int Read(char *buf, int size, uint64_t pos, bool direct) override;
  int Write(char *data, uint32_t size, uint64_t pos) override;
  int InvalidateCache(uint64_t pos, uint64_t size) override;
  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                      unsigned int idx) override;
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                    unsigned int idx) override;
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                     unsigned int idx) override;
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                           unsigned int idx) override;
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx) override;

 protected:
  void syncBackendInfo() override;
};
}  // namespace aquafs

#endif  // ROCKSDB_ZONE_RAIDC_H
