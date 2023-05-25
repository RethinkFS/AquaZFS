//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_CODING_H
#define AQUAFS_CODING_H

#include <cstdint>
#include "slice.h"
#include "coding_lean.h"

namespace aquafs {
extern bool GetFixed64(Slice *input, uint64_t *value);

extern bool GetFixed32(Slice *input, uint32_t *value);

extern bool GetFixed16(Slice *input, uint16_t *value);

extern void PutFixed64(std::string *dst, uint64_t value);

extern void PutFixed32(std::string *dst, uint32_t value);

extern void PutLengthPrefixedSlice(std::string *dst, const Slice &value);

extern bool GetLengthPrefixedSlice(Slice *input, Slice *result);
}

#endif //AQUAFS_CODING_H
