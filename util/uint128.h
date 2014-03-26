#pragma once

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
  static constexpr __int128_t __min =
      static_cast<__int128_t>(static_cast<__uint128_t>(1) << digits);
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


std::ostream &operator<<(std::ostream &s, const __uint128_t &h);

