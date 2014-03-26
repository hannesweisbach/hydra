#include "util/uint128.h"

std::ostream &operator<<(std::ostream &s, const __uint128_t &h) {
  if (h)
    return s << std::hex << "0x" << (uint64_t)(h >> 64)
             << (uint64_t)(h & 0xffffffffffffffff) << std::dec;
  else
    return s << "0x0000000000000000000000000000000";
}

