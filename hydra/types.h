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

struct node_id {
  keyspace_t id;
  // uint32_t ip;
  char ip[16];
  // uint16_t port;
  char port[6];
  node_id() = default;
  node_id(const keyspace_t &id, const char (&ip_)[16], const char (&port_)[6])
      : id(id) {
    memcpy(ip, ip_, sizeof(ip));
    memcpy(port, port_, sizeof(port));
  }
  node_id(const keyspace_t &id, const std::string &ip_,
          const std::string &port_)
      : id(id), ip(), port() {
    ip_.copy(ip, sizeof(ip));
    port_.copy(port, sizeof(port));
  }
};

struct routing_entry {

  node_id node;
  keyspace_t start;
  routing_entry() {}
  routing_entry(const node_id &node, const struct keyspace_t &start)
      : node(node), start(start) {}
  routing_entry(const std::string &ip, const std::string &port,
                const keyspace_t &n, const keyspace_t &k)
      : node(n, ip, port), start(n + (keyspace_t(1) << k)) {}
  routing_entry(const std::string &ip, const std::string &port,
                const keyspace_t &n)
      : node(n, ip, port), start(n) {}
};

/* we have an identifier space of 2**128.
 * chord says we need a routing table of size 128.
 * I additionally store my predecessor and myself in the table.
 * [0] is my predecessor
 * [1] is myself
 * [2] is my successor
 * and so forth
 * TODO: put predecessor in slot 128., so the layout of the array corresponds to
 * chord.
 */
constexpr size_t routingtable_size =
    std::numeric_limits<keyspace_t::value_type>::digits + 2;

struct routing_table {
  static const size_t predecessor_index = 0;
  static const size_t self_index = 1;
  static const size_t successor_index = 2;

  std::array<routing_entry, routingtable_size> table;

  static_assert(routingtable_size > successor_index, "Table is not large enough");

  auto begin() const { return &successor(); }
  auto begin() { return &successor(); }
  auto end() const { return table.end(); }
  auto end() { return table.end(); }

  routing_table() = default;
  routing_table(const std::string &ip, const std::string &port,
                const keyspace_t &id) {
    table[predecessor_index] = routing_entry(ip, port, id);
    table[self_index] = routing_entry(ip, port, id);
    keyspace_t::value_type k = 0;
    for (auto &&entry : *this) {
      entry = routing_entry(ip, port, id, k++);
    }
  }
  routing_table(const std::string &ip, const std::string &port)
      : routing_table(ip, port, keyspace_t(static_cast<keyspace_t::value_type>(
                                    hash(ip)))) {}

  const routing_entry preceding_node(const keyspace_t &id) const {
#if 0
    log_info() << std::hex << "Checking (" << (unsigned)self().node.id << " "
               << (unsigned)id << ") contains ";
#endif
    auto it = std::find_if(table.rbegin(), table.rend() + 2, [=](const auto &node) {
// return node.interval.contains(id);
#if 0
      log_info() << std::hex <<  "  " << (unsigned)node.node.id;
#endif
#if 1
      //gcc fails to call this->self(), so call it explicitly
      return node.node.id.in(this->self().node.id + 1, id - 1);
#else
      return interval({ static_cast<keyspace_t>(),
                        static_cast<keyspace_t>(id - 1) })
          .contains(node.node.id);
#endif
    });
    assert(it != table.rend() + 2);

    return *it;
  }

  bool has_id(const keyspace_t &id) const {
    return id.in(predecessor().node.id + 1, self().node.id);
  }

  routing_entry &operator[](const size_t i) {
    return table[i + successor_index];
  }
  const routing_entry &operator[](const size_t i) const {
    return table[i + successor_index];
  }

  const routing_entry &predecessor() const { return table[predecessor_index]; }
  routing_entry &predecessor() { return table[predecessor_index]; }
  const routing_entry &self() const { return table[self_index]; }
  routing_entry &self() { return table[self_index]; }
  const routing_entry &successor() const { return table[successor_index]; }
  routing_entry &successor() { return table[successor_index]; }

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
  hash_table_entry() noexcept : hash_table_entry(nullptr, 0, 0, 0, 0) {}
  bool is_empty() const { return ptr.is_empty(); }
  void empty() {
    key_size = 0;
    rkey = 0;
    hop = 0;
    ptr.reset();
  }
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
std::ostream &operator<<(std::ostream &s, const hydra::node_id &id);
std::ostream &operator<<(std::ostream &s, const hydra::routing_entry &e);
std::ostream &operator<<(std::ostream &s, const hydra::routing_table &t);

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

