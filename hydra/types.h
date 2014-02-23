#pragma once

#include <rdma/rdma_cma.h>

#include "verifying_ptr.h"


namespace hydra {

struct node_info {
  __uint128_t id;
  uint64_t table_size;
  ibv_mr key_extents;
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

struct key_entry {
  verifying_ptr<unsigned char> ptr;
  size_t key_size;
  uint32_t hop;
  uint32_t rkey;
  uint64_t crc;

  key_entry(const unsigned char *p = nullptr, const size_t size = 0,
            const size_t key_size = 0, const uint32_t rkey = 0) noexcept
      : ptr(p, size),
        key_size(key_size),
        hop(0),
        rkey(rkey) {
    rehash();
  }
#if 0
  key_entry(key_entry &&rhs)
      : ptr(std::move(rhs.ptr)), key_size(std::move(rhs.key_size)),
        hop(std::move(rhs.hop)), crc(std::move(rhs.crc)) {}
#endif
  //  key_entry& operator=(key_entry&& rhs) = default;
  //  key_entry& operator=(const key_entry& rhs) = default;

  // TODO add (optimized) template overload/specialization
  void rehash() { crc = ::hydra::hash64(this, sizeof(*this) - sizeof(crc)); }
  bool is_empty() const { return ptr.is_empty(); }
  void empty() {
    key_size = 0;
    rkey = 0;
    ptr.reset();
    rehash();
  }
  bool has_key(const char *k, size_t klen) const {
    return (key_size == klen) && (memcmp(k, ptr.get(), key_size) == 0);
  }
  void set_hop(size_t i) {
    hop |= (1 << i);
    rehash();
  }
  void clear_hop(size_t i) {
    hop &= ~(1 << i);
    rehash();
  }
  const unsigned char *key() const { return ptr.get(); }
  const unsigned char *value() const { return ptr.get() + key_size; }
  size_t key_length() const { return key_size; }
  size_t value_length() const { return ptr.size - key_size; }
  bool valid() const {
    return ::hydra::hash64(this, sizeof(*this) - sizeof(crc)) == crc;
  }
};
std::ostream &operator<<(std::ostream &s, const hydra::key_entry &e);
}
