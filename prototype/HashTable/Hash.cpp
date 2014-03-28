#include "Hash.hpp"

#include <ostream>

std::ostream &operator<<(std::ostream &s, const __uint128_t &h) {
  return s << std::hex << "0x" << (uint64_t)(h >> 64)
           << (uint64_t)(h & 0xffffffffffffffff) << std::dec;
}
