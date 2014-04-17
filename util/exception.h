#pragma once

#include <stdexcept>
#include <string>
#include <cstring>
#include <sstream>

#include "util/Logger.h"

[[noreturn]] inline void throw_errno() {
  throw std::runtime_error(strerror(errno));
}

[[noreturn]] inline void throw_errno(const std::string& where) {
  log_error() << where << ": " << strerror(errno);
  throw std::runtime_error(where + ": " + strerror(errno));
}


template<typename T>
inline T* check_nonnull(T* ptr, const std::string& s = "") {
  if(ptr == nullptr)
    throw_errno(s);
  return ptr;
}

#if __clang__

#define check_zero(x)                                                          \
  _Pragma("clang diagnostic push")                                             \
  _Pragma("clang diagnostic ignored \"-Wgnu-statement-expression\"")           \
  ({                                                                           \
    auto tmp = x;                                                              \
    if (tmp) {                                                                 \
      std::ostringstream ss;                                                   \
      ss << __func__ << "(" << __LINE__ << ")";                                \
      throw_errno(ss.str());                                                   \
    };                                                                         \
    tmp;                                                                       \
  })                                                                           \
  _Pragma("clang diagnostic pop")

#else

#define check_zero(x)                                                          \
  ({                                                                           \
    auto tmp = x;                                                              \
    if (tmp) {                                                                 \
      std::ostringstream ss;                                                   \
      ss << __func__ << "(" << __LINE__ << ")";                                \
      throw_errno(ss.str());                                                   \
    };                                                                         \
    tmp;                                                                       \
  })

#endif
