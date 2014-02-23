#pragma once

#include <memory>
#include <cstdint>
#include "hash.h"
#include "util/exception.h"

template <typename T> struct verifying_ptr {
  uint64_t ptr;
  //(u)int32_t might suffice
  size_t size;
  uint64_t crc;

  verifying_ptr(const T *p = nullptr, size_t s = 0) noexcept
      : ptr(reinterpret_cast<uint64_t>(p)), size(s), crc(::hydra::hash64(p, s)) {
#if 0
    cocoa::hash h = cocoa::hash::CRC64(p, s);
    crc = ((uint64_t)h[0] << 32) | h[1];
#else
//    update();
#endif
  }
#if 0
  verifying_ptr(size_t s)
      : ptr(reinterpret_cast<uint64_t>(check_nonnull(tc_malloc(size)))),
        size(s), crc(0) {}
  ~verifying_ptr() {
    tc_free(reinterpret_cast<void*>(ptr));
  }
  verifying_ptr(const verifying_ptr& rhs) = default;
  verifying_ptr(verifying_ptr &&rhs)
      : ptr(std::move(rhs.ptr)), size(std::move(rhs.size)),
        crc(std::move(rhs.crc)) {
        rhs.ptr = reinterpret_cast<uint64_t>(nullptr);
  }
#endif
  const T *get() const { return reinterpret_cast<T *>(ptr); }
  bool is_empty() const { return reinterpret_cast<T *>(ptr) == nullptr; }
  void reset() {
    ptr = reinterpret_cast<uint64_t>(nullptr);
    size = 0;
  }
//  void update() { crc = hash64(reinterpret_cast<void*>(ptr), size); }
};

