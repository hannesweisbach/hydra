#pragma once

#include <algorithm>
#include <ostream>
#include <initializer_list>
#include <cstring>

#include <rdma/rdma_cma.h>

#include "hydra/keyspace.h"
#include "verifying_ptr.h"
#include "util/uint128.h"
#include "util/Logger.h"
#include "util/utils.h"

#include "hydra/RDMAObj.h"

namespace hydra {


std::ostream &operator<<(std::ostream &, const keyspace_t &rhs);

namespace literals {
constexpr keyspace_t operator"" _ID(unsigned long long id) {
  return keyspace_t(static_cast<keyspace_t::value_type>(id));
}
}

using namespace hydra::literals;

struct node_info {
  keyspace_t id;
  uint64_t table_size;
  ibv_mr key_extents;
  ibv_mr routing_table;
// routing/other nodes
#if 0
  struct value_extents_info {
    verifying_ptr<mr> ptr;
    mr p;
    uint64_t crc;
  };
#endif
};


enum Return_t {
  SUCCESS,
  NOTFOUND,
  NEED_RESIZE
};

struct hash_table_entry {
  verifying_ptr<unsigned char> ptr;
  size_t key_size;
  uint32_t hop;
  uint32_t rkey;

  hash_table_entry(const unsigned char *p, const size_t size,
                   const size_t key_size, const uint32_t rkey,
                   const uint32_t hop) noexcept : ptr(p, size),
                                                  key_size(key_size),
                                                  hop(hop),
                                                  rkey(rkey) {}
  hash_table_entry &operator=(hash_table_entry && other) {
    ptr = std::move(other.ptr);
    key_size = other.key_size;
    other.key_size = 0;
    rkey = other.rkey;
    other.rkey = 0;
    return *this;
  }
  hash_table_entry &operator=(const hash_table_entry &other) {
    ptr = other.ptr;
    key_size = other.key_size;
    rkey = other.rkey;
    return *this;
  }
  hash_table_entry() noexcept : hash_table_entry(nullptr, 0, 0, 0, 0) {}
  void empty() {
    ptr = { nullptr, 0 };
    key_size = 0;
    rkey = 0;
  }
  bool is_empty() const { return ptr.is_empty(); }
  operator bool() const noexcept { return !is_empty(); }
  bool has_key(const char *k, size_t klen) const {
    return (key_size == klen) && (memcmp(k, ptr.get(), key_size) == 0);
  }
  void set_hop(size_t i) { hop |= (1 << i); }
  void clear_hop(size_t i) { hop &= ~(1 << i); }
  const unsigned char *key() const { return ptr.get(); }
  const unsigned char *value() const { return ptr.get() + key_size; }
  size_t key_length() const { return key_size; }
  size_t value_length() const { return ptr.size - key_size; }
};

using server_entry = LocalRDMAObj<hash_table_entry>;

std::ostream &operator<<(std::ostream &s, const hydra::hash_table_entry &e);

template <typename T> struct hex_wrapper {
  T v;
  hex_wrapper(const T &v) : v(v) {}
};

template <typename T> hex_wrapper<T> hex(const T &t) {
  return hex_wrapper<T>({ t });
}

template <typename T>
std::ostream &operator<<(std::ostream &s, const hex_wrapper<T> hex) {
  auto fmtflags = s.flags();
  s << hex.v;
  s.flags(fmtflags);
  return s;
}

template <>
std::ostream &operator<<(std::ostream &s, const hex_wrapper<uint8_t> hex);
}

struct mr {
  uint64_t addr;
  uint32_t size;
  uint32_t rkey;
  mr() noexcept : addr(0), size(0), rkey(0) {}
  mr(ibv_mr *mr) noexcept : addr(reinterpret_cast<uint64_t>(mr->addr)),
                            size(static_cast<uint32_t>(mr->length)),
                            rkey(mr->rkey) {}
  template <typename T>
  mr(const T *const p, size_t size, uint32_t rkey) noexcept
      : addr(reinterpret_cast<uint64_t>(p)),
        size(static_cast<uint32_t>(size)),
        rkey(rkey) {}
};

std::ostream &operator<<(std::ostream &s, const mr &mr);

