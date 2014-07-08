#pragma once

#include <cstdint>
#include <type_traits>
#include <limits>
#include <string>
#include <ostream>
#include <vector>

#include <city.h>

#include "hydra/keyspace.h"

namespace hydra {
template <typename T>
inline hydra::keyspace_t::value_type hash(const T *s, size_t len) {
  uint128 h = CityHash128(reinterpret_cast<const char *>(s), len);
  return static_cast<hydra::keyspace_t::value_type>(
      static_cast<__uint128_t>(Uint128High64(h))
          << std::numeric_limits<std::uint64_t>::digits |
      static_cast<__uint128_t>(Uint128Low64(h)));
}

template <typename T>
inline uint64_t do_hash64_(const T &s, size_t size, std::true_type) {
  return CityHash64(reinterpret_cast<const char *>(s), size);
}

template <typename T>
inline uint64_t do_hash64_(const T &s, size_t size, std::false_type) {
  return CityHash64(reinterpret_cast<const char *>(&s), size);
}

template <typename T>
inline uint64_t hash64(const T &s, size_t size = sizeof(T)) {
  return do_hash64_(s, size, std::is_pointer<T>());
}

template <typename T>
inline hydra::keyspace_t::value_type do_hash_(const T &s, size_t size,
                                              std::true_type) {
  return hash(s, size);
}

template <typename T>
inline hydra::keyspace_t::value_type do_hash_(const T &s, size_t size,
                                              std::false_type) {
  return hash(&s, size);
}

template <typename T>
inline hydra::keyspace_t::value_type hash(const T &s, size_t size = sizeof(T)) {
  return do_hash_(s, size, std::is_pointer<T>());
}

template <>
inline hydra::keyspace_t::value_type hash<std::string>(const std::string &s,
                                                       size_t) {
  return hash(s.c_str(), s.size());
}

template <typename T>
inline hydra::keyspace_t::value_type hash(const std::vector<T> &v) {
  return hash(std::begin(v), v.size() * sizeof(T));
}
}

