#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <ostream>

namespace hydra {
namespace util {
template <size_t val> struct static_log2 {
  static_assert(val != 0, "log2(0) is undefined.");
  static constexpr size_t value = 1 + static_log2<val / 2>::value;
};
template <> struct static_log2<1> {
  static constexpr size_t value = 0;
};
constexpr size_t log2_(const size_t val, const size_t log = 0) {
  return (val <= 1) ? log : log2_(val/2, log + 1);
}
inline uint64_t log2(const uint64_t val) {
  uint64_t log2;
  asm("bsrq %1,%0" : "=r"(log2) : "r"(val));
  return log2;
}
}
}

/* As of libstdc++-4.9 std::max is not yet constexpr */
template <typename... Args> constexpr size_t sizeof_largest_type() {
  size_t max = 0;
  for (const auto &size : { sizeof(Args)... }) {
    if (size > max)
      max = size;
  }
  return max;
}

template <size_t size> class is_power_of_two {
public:
  static constexpr bool value = (size & (size - 1)) == 0;
};

