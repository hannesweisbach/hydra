#pragma once

#include <memory>
#include <cstdint>
#include "hash.h"

template <typename T> struct verifying_ptr {
  uint64_t ptr;
  //(u)int32_t might suffice
  size_t size;
  uint64_t crc;

  verifying_ptr(const T *p = nullptr, size_t s = 0) noexcept
      : ptr(reinterpret_cast<uint64_t>(p)),
        size(s),
        crc(::hydra::hash64(p, s)) {}
  const T *get() const { return reinterpret_cast<T *>(ptr); }
  bool is_empty() const { return reinterpret_cast<T *>(ptr) == nullptr; }
  void reset() {
    ptr = reinterpret_cast<uint64_t>(nullptr);
    size = 0;
  }
  //  void update() { crc = hash64(reinterpret_cast<void*>(ptr), size); }
};

