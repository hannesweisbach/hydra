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
                const keyspace_t &start, const keyspace_t &id)
      : node(id, ip, port), start(start) {}
  routing_entry(const std::string &ip, const std::string &port,
                const keyspace_t &id)
      : node(id, ip, port), start(id) {}
  bool empty() const { return node.ip[0] == 0; }
};

using entry_t = LocalRDMAObj<routing_entry>;

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

std::unique_ptr<network> connect(const std::string &host,
                                 const std::string &port);

class routing_table {
  virtual kj::Array<capnp::word> init() const = 0;
  virtual kj::Array<capnp::word> process_join(const std::string &host,
                                              const std::string &port) = 0;
  virtual void update(const std::string &host, const std::string &port,
                      const keyspace_t &id, const size_t index) = 0;

protected:
  const std::string local_host;
  const std::string local_port;

public:
  routing_table(const std::string &host, const std::string &port);
  routing_table(const routing_table &) = default;
  virtual ~routing_table() = default;
  routing_table& operator=(routing_table&& ) = default;
  routing_table &operator=(const routing_table &) = default;

  virtual std::pair<keyspace_t, keyspace_t> join(const std::string &host,
                                                 const std::string &port) = 0;
  kj::Array<capnp::word>
  process_message(const hydra::protocol::DHTRequest::Overlay::Reader &);
};

std::ostream &operator<<(std::ostream &s, const node_id &id);
std::ostream &operator<<(std::ostream &s, const routing_entry &e);

kj::Array<capnp::word> network_request();
void init_node(const std::string &host, const std::string &port,
               hydra::protocol::Node::Builder &n);
kj::Array<capnp::word> join_request(const std::string &host,
                                    const std::string &port);
kj::Array<capnp::word> join_reply(const keyspace_t &start,
                                  const keyspace_t &end,
                                  const bool success = true);
kj::Array<capnp::word> update_message(const std::string &host,
                                      const std::string &port,
                                      const keyspace_t &id, const size_t index);
}
}
