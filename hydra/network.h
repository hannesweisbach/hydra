#pragma once

#include <utility>
#include <string>
#include <memory>

#include <capnp/serialize.h>

#include "hydra/keyspace.h"
#include "hydra/types.h"
#include "hydra/passive.h"

namespace hydra {
namespace overlay {
using namespace hydra::literals;
class network {
public:
  class node {
    std::pair<keyspace_t, keyspace_t> range;
    std::unique_ptr<hydra::passive> node_;

  public:
    node(const keyspace_t &start, const keyspace_t &end,
         const std::string &host, const std::string &port)
        : range(start, end),
          node_(std::make_unique<hydra::passive>(host, port)) {}
    bool contains(const keyspace_t &id) const {
      return id.in(range.first, range.second);
    }
    operator hydra::passive &() { return *node_; }
  };
  virtual ~network() = default;

  virtual passive &successor(const keyspace_t &id) = 0;
};

class routing_table {
public:
  routing_table() = default;
  routing_table(const routing_table &) = default;
  virtual ~routing_table() = default;
  routing_table& operator=(routing_table&& ) = default;
  routing_table &operator=(const routing_table &) = default;

  virtual kj::Array<capnp::word> init() const = 0;
  //virtual new_keyspace_range join(nodeid) = 0;
};

struct node_id {
  keyspace_t id;
  char ip[16];
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
      : node(n, ip, port), start(n + (1_ID << k)) {}
  routing_entry(const std::string &ip, const std::string &port,
                const keyspace_t &n)
      : node(n, ip, port), start(n) {}
};

std::ostream &operator<<(std::ostream &s, const node_id &id);
std::ostream &operator<<(std::ostream &s, const routing_entry &e);

}
}
