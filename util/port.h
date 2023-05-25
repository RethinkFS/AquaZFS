//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_PORT_H
#define AQUAFS_PORT_H

#include <endian.h>

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

namespace aquafs::port {
constexpr bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
}

#endif //AQUAFS_PORT_H
