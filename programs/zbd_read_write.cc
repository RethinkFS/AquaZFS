//
// Created by chiro on 23-6-4.
//

#include "fs/tools/tools.h"
#include "fs/zbdlib_aquafs.h"

using namespace aquafs;

int main() {
  prepare_test_env(2);
  // open nullb0
  auto zbd = std::make_unique<ZonedBlockDevice>(
      "nullb0", ZbdBackendType::kBlockDev, nullptr);
  auto open_status = zbd->Open(false, true);
  assert(open_status.ok());
  // reset zone
  idx_t zone = 0x0;
  // idx_t zone = 0x3;
  bool tmp_offline = false;
  uint64_t tmp_max_capacity;
  // assert(zbd->getBackend()
  //            ->Reset(zone * zbd->GetZoneSize(), &tmp_offline,
  //            &tmp_max_capacity) .ok());
  // auto sz = 512ul;
#ifdef ROCKSDB_USE_RTTI
  auto be = dynamic_cast<ZbdlibBackend *>(zbd->getBackend().get());
  assert(be);
#else
  auto be = (ZbdlibBackend *)(zbd->getBackend().get());
#endif
  printf("write_f_: %x\n", be->write_f_);

  auto sz = 0x40000;
  // WARNING: cannot use `new` to create direct io access memory
  // auto data = new char[sz];
  char *data = nullptr;
  assert(posix_memalign((void **)(&data), getpagesize(), sz) == 0);
  auto pos = zone * zbd->GetZoneSize();
  auto read_sz =
      zbd->getBackend()->Read(data, static_cast<int>(sz), pos, false);
  if (read_sz < 0) printf("read err: %s\n", strerror(errno));
  assert(read_sz > 0);

  auto zones = zbd->getBackend()->ListZones();
  auto wp = zbd->getBackend()->ZoneWp(zones, zone);
  auto start = zbd->getBackend()->ZoneStart(zones, zone);
  printf("wp: %lx, pos: %lx, start: %lx\n", wp, pos, start);
  auto status = zbd->getBackend()->Reset(pos, &tmp_offline, &tmp_max_capacity);
  assert(status.ok());
  int written = -1;
  written = zbd->getBackend()->Write(data, sz, wp);
  if (written < 0) printf("write err: %s\n", strerror(errno));
  if (written < 0 && data) {
    // retry with pwrite
    written = pwrite(be->write_f_, data, sz, wp);
    if (written < 0) printf("pwrite err: %s\n", strerror(errno));
  }
  fflush(stdout);
  assert(written > 0);
  auto zbd2 = std::make_unique<ZonedBlockDevice>(
      "nullb1", ZbdBackendType::kBlockDev, nullptr);
  return 0;
}