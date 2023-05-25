//
// Created by chiro on 23-5-25.
//

#ifndef AQUAFS_CODING_H
#define AQUAFS_CODING_H

#include <cstdint>
#include "slice.h"
#include "coding_lean.h"

namespace aquafs {

const uint32_t kMaxVarint64Length = 10;

extern bool GetFixed64(Slice *input, uint64_t *value);

extern bool GetFixed32(Slice *input, uint32_t *value);

extern bool GetFixed16(Slice *input, uint16_t *value);

extern void PutFixed64(std::string *dst, uint64_t value);

extern void PutFixed32(std::string *dst, uint32_t value);

extern void PutLengthPrefixedSlice(std::string *dst, const Slice &value);

extern bool GetLengthPrefixedSlice(Slice *input, Slice *result);

extern char* EncodeVarint64(char* dst, uint64_t value);
}

#endif //AQUAFS_CODING_H
