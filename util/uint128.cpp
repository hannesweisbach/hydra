#include "util/uint128.h"

std::ostream &std::operator<<(std::ostream &s, const __uint128_t &h) {
  auto flags = s.flags();
  if (h)
    s << std::hex << "0x" << std::setw(16) << static_cast<uint64_t>(h >> 64)
      << std::setw(16) << static_cast<uint64_t>(h & 0xffffffffffffffff);
  else
    s << "0x0000000000000000000000000000000";
  s.flags(flags);
  return s;
}

