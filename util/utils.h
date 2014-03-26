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

template <typename T> constexpr size_t sizeof_largest_type() {
  return sizeof(T);
}

#ifndef __has_feature         // Optional of course.
  #define __has_feature(x) 0  // Compatibility with non-clang compilers.
#endif

template <typename T1, typename T2, typename... Args>
constexpr size_t sizeof_largest_type() {
#if __has_feature(cxx_relaxed_constexpr)
  constexpr size_t s1 = sizeof_largest_type<T1>();
  constexpr size_t s2 = sizeof_largest_type<T2, Args...>();
  return (s1 > s2) ? s1 : s2;
#else
  return (sizeof_largest_type<T1>() > sizeof_largest_type<T2, Args...>())
             ? sizeof_largest_type<T1>()
             : sizeof_largest_type<T2, Args...>();
#endif
}

template <size_t size> class is_power_of_two {
public:
  static constexpr bool value = (size & (size - 1)) == 0;
};

