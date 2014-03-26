#pragma once

#include <algorithm>
#include <ostream>
#include <initializer_list>
#include <cstring>

#include <rdma/rdma_cma.h>

#include "verifying_ptr.h"
#include "util/uint128.h"
#include "util/Logger.h"

namespace hydra {

struct node_info {
  __uint128_t id;
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

struct node_id {
  __uint128_t id;
  // uint32_t ip;
  char ip[16];
  // uint16_t port;
  char port[6];
  node_id() = default;
  node_id(const __uint128_t &id, const char (&ip_)[16], const char (&port_)[6])
      : id(id) {
    memcpy(ip, ip_, sizeof(ip));
    memcpy(port, port_, sizeof(port));
  }
  node_id(const __uint128_t &id, const std::string &ip_,
          const std::string &port_)
      : id(id), ip(), port() {
    ip_.copy(ip, sizeof(ip));
    port_.copy(port, sizeof(port));
  }
};

struct interval {
  __uint128_t start;
  __uint128_t end;

  bool contains(const __uint128_t &v) const {
    return (v - start) < (end - start);
  }
};

struct routing_entry {
  node_id node;
  interval interval;
  node_id successor;
  routing_entry(const node_id &node, const struct interval &interval)
      : node(node), interval(interval) {}
  routing_entry(const std::string &ip, const std::string &port,
                const __uint128_t &n = 0, const size_t k = 0)
      : node(n, ip, port), interval({n + (1 << (k - 1)), n + (1 << k)}),
        successor({ 0, { 0 }, { 0 } }) {}
};

constexpr size_t routingtable_size = std::numeric_limits<__uint128_t>::digits;

struct routing_table {
  // todo own array, with unique_ptr  as parameter -> RDMA
  std::array<routing_entry, routingtable_size> table;

  const routing_entry preceding_node(const __uint128_t &id) const {
    auto it =
        std::find_if(table.rbegin(), table.rend(),
                     [=](auto &node) { return node.interval.contains(id); });
    assert(it != table.rend());

    return *it;
  }
  routing_entry &operator[](const size_t i) { return table[i]; }
  const routing_entry &operator[](const size_t i) const { return table[i]; }
  const routing_entry &predecessor() const { return table[0]; }
  routing_entry &predecessor() { return table[0]; }
  const routing_entry &successor() const { return table[1]; }
  routing_entry &successor() { return table[1]; }
  auto end() const { return table.end(); }
  auto end() { return table.end(); }
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
std::ostream &operator<<(std::ostream &s, const hydra::node_id &id);
std::ostream &operator<<(std::ostream &s, const hydra::interval &i);
std::ostream &operator<<(std::ostream &s, const hydra::routing_entry &e);
std::ostream &operator<<(std::ostream &s, const hydra::routing_table &t);
}


