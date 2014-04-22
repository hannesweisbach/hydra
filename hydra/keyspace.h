#pragma once

#include <limits>
#include "util/utils.h"

namespace hydra {

struct keyspace_t {
  using value_type = uint8_t;

  static_assert(std::numeric_limits<value_type>::digits,
                "You have to implement std::numeric_limits<keyspace_t>.");

  static_assert(
      is_power_of_two<std::numeric_limits<value_type>::digits>::value,
      "std::numeric_limits<keyspace_t>::digits must be a power of two.");

  value_type value__;

  keyspace_t() = default;
  keyspace_t(const keyspace_t &) = default;
  keyspace_t(keyspace_t &&) = default;
  keyspace_t &operator=(const keyspace_t &) = default;
  keyspace_t &operator=(keyspace_t &&) = default;
  constexpr keyspace_t(value_type v) noexcept : value__(v) {}

  keyspace_t operator+(keyspace_t const &rhs) const noexcept {
    return keyspace_t(value__ + rhs.value__);
  }

  keyspace_t operator-(keyspace_t const &rhs) const noexcept {
    return keyspace_t(value__ - rhs.value__);
  }

  keyspace_t operator<<(keyspace_t const &rhs) const noexcept {
    return keyspace_t(static_cast<value_type>(value__ << rhs.value__));
  }

  bool operator==(keyspace_t const &rhs) const noexcept {
    return value__ == rhs.value__;
  }
  bool operator!=(keyspace_t const &rhs) const noexcept {
    return value__ != rhs.value__;
  }
  bool operator<=(keyspace_t const &rhs) const noexcept {
    return value__ <= rhs.value__;
  }
  bool operator>=(keyspace_t const &rhs) const noexcept {
    return value__ >= rhs.value__;
  }
  bool operator<(keyspace_t const &rhs) const noexcept {
    return value__ < rhs.value__;
  }
  bool operator>(keyspace_t const &rhs) const noexcept {
    return value__ > rhs.value__;
  }

  bool in(keyspace_t const &start, keyspace_t const &end) const noexcept {
    if (end == start)
      return start == *this;
    else
      return (*this - start) <= (end - start);
  }
};
}