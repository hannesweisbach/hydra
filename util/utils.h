#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <ostream>
#include <limits>
#include <type_traits>

namespace std {
template <> class is_arithmetic<__int128_t> : public true_type {};
template <> class is_arithmetic<__uint128_t> : public true_type {};

template <> struct numeric_limits<__uint128_t> {
  static constexpr bool is_specialized = true;
  static constexpr bool is_signed = false;
  static constexpr int digits =
      static_cast<int>(sizeof(__uint128_t) * __CHAR_BIT__ - is_signed);
  static constexpr int digits10 = digits * 3 / 10;
  static constexpr int max_digits10 = 0;
  static constexpr __uint128_t __min = 0;
  static constexpr __uint128_t __max =
      is_signed ? __uint128_t(__uint128_t(~0) ^ __min) : __uint128_t(~0);
  static constexpr __uint128_t min() { return __min; }
  static constexpr __uint128_t max() { return __max; }
  static constexpr __uint128_t lowest() { return min(); }
  static constexpr bool is_integer = true;
  static constexpr bool is_exact = true;
  static constexpr int radix = 2;
  static constexpr __uint128_t epsilon() { return 0; }
  static constexpr __uint128_t round_error() { return 0; }
  static constexpr int min_exponent = 0;
  static constexpr int min_exponent10 = 0;
  static constexpr int max_exponent = 0;
  static constexpr int max_exponent10 = 0;
  static constexpr bool has_infinity = false;
  static constexpr bool has_quiet_NaN = false;
  static constexpr bool has_signaling_NaN = false;
  static constexpr float_denorm_style has_denorm = denorm_absent;
  static constexpr bool has_denorm_loss = false;
  static constexpr __uint128_t infinity() { return 0; }
  static constexpr __uint128_t quiet_NaN() { return 0; }
  static constexpr __uint128_t signaling_NaN() { return 0; }
  static constexpr __uint128_t denorm_min() { return 0; }
  static const bool is_iec559 = false;
  static const bool is_bounded = true;
  static const bool is_modulo = true;
  static const bool traps = false;
  static const bool tinyness_before = false;
  static const float_round_style round_style = round_toward_zero;
};
template <> struct numeric_limits<__int128_t> {
  static constexpr bool is_specialized = true;
  static constexpr bool is_signed = true;
  static constexpr int digits =
      static_cast<int>(sizeof(__int128_t) * __CHAR_BIT__ - is_signed);
  static constexpr int digits10 = digits * 3 / 10;
  static constexpr int max_digits10 = 0;
  static constexpr __int128_t __min = static_cast<__int128_t>(1) << digits;
  static constexpr __int128_t __max =
      is_signed ? __int128_t(__int128_t(~0) ^ __min) : __int128_t(~0);
  static constexpr __int128_t min() { return __min; }
  static constexpr __int128_t max() { return __max; }
  static constexpr __int128_t lowest() { return min(); }
  static constexpr bool is_integer = true;
  static constexpr bool is_exact = true;
  static constexpr int radix = 2;
  static constexpr __int128_t epsilon() { return 0; }
  static constexpr __int128_t round_error() { return 0; }
  static constexpr int min_exponent = 0;
  static constexpr int min_exponent10 = 0;
  static constexpr int max_exponent = 0;
  static constexpr int max_exponent10 = 0;
  static constexpr bool has_infinity = false;
  static constexpr bool has_quiet_NaN = false;
  static constexpr bool has_signaling_NaN = false;
  static constexpr float_denorm_style has_denorm = denorm_absent;
  static constexpr bool has_denorm_loss = false;
  static constexpr __int128_t infinity() { return 0; }
  static constexpr __int128_t quiet_NaN() { return 0; }
  static constexpr __int128_t signaling_NaN() { return 0; }
  static constexpr __int128_t denorm_min() { return 0; }
  static const bool is_iec559 = false;
  static const bool is_bounded = true;
  static const bool is_modulo = true;
  static const bool traps = false;
  static const bool tinyness_before = false;
  static const float_round_style round_style = round_toward_zero;
};
}

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

