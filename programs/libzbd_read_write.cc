//
// Created by chiro on 23-6-4.
//

#include <fcntl.h>
#include <libzbd/zbd.h>

#include "fs/tools/tools.h"

using namespace aquafs;

int main() {
  prepare_test_env(2);
  int f;
  zbd_info info;
  f = zbd_open("/dev/nullb0", O_WRONLY | O_DIRECT, &info);
  if (f < 0) {
    printf("Failed to open libzbd context\n");
    return 1;
  }
  const auto sz = 2048u;
  // char *data[sz];
  char *data = nullptr;
  assert(posix_memalign((void **)(&data), getpagesize(), sz) == 0);
  if (data) assert(pwrite(f, data, sz, 0) > 0);
  return 0;
}