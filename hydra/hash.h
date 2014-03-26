#pragma once 

#include <cstdint>
#include <type_traits>
#include <limits>
#include <string>
#include <ostream>

#include <city.h>

namespace hydra {
template <typename T>
inline __uint128_t hash(const T * s, size_t len) {
  uint128 h = CityHash128(reinterpret_cast<const char *>(s), len);
  return (
             __uint128_t)Uint128High64(h)
             << std::numeric_limits<std::uint64_t>::digits |
         Uint128Low64(h);
}

template <typename T>
inline uint64_t hash64(const T * s, size_t size = sizeof(T)) {
  return CityHash64(reinterpret_cast<const char *>(s), size);
}

template <typename T, typename = typename std::enable_if<
                          !std::is_pointer<T>::value, T>::type>
inline uint64_t hash64(const T &s, size_t size = sizeof(T)) {
  return hash64(&s, size);
}

template <typename T, typename = typename std::enable_if<
                          !std::is_pointer<T>::value, T>::type>
inline __uint128_t hash(const T& s, size_t size = sizeof(T)) {
  return hash(&s, size);
}

template<>
inline __uint128_t hash<std::string>(const std::string& s, size_t) {
  return hash((char*)s.c_str(), s.size());
}
}


