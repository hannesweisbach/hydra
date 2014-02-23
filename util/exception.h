#pragma once

#include <stdexcept>
#include <string>
#include <cstring>

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

template<typename T>
inline void check_zero(T result, const std::string& s = "") {
  if(result)
    throw_errno(s);
}

