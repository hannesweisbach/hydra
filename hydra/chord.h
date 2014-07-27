#pragma once

#include <string>
#include <memory>
#include <vector>

#include "rdma/RDMAClientSocket.h"
#include "rdma/RDMAServerSocket.h"
#include "hydra/network.h"

namespace hydra {
namespace overlay {
namespace chord {

//decltype(heap.malloc<LocalRDMAObj<routing_table>>()) routing_table_;
#if 0
  /* call when joining the network - already running node ip */
  void init_routing_table(const hydra::overlay::chord& remote);
  void update_others() const;
  void update_routing_table(const hydra::node_id &e, const size_t i);
  void update_predecessor(const hydra::overlay::node_id &pred) const;

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  // no reply.
  send(predecessor_message(pred));
}

#endif

class routing_table : public hydra::overlay::routing_table {
  kj::Array<capnp::word> init() const override;
  kj::Array<capnp::word> process_join(const std::string &host,
                                      const std::string &port) override;
  void update(const std::string &host, const std::string &port,
              const keyspace_t &id, const size_t index) override;
  std::pair<keyspace_t, keyspace_t> join(const std::string &host,
                                         const std::string &port) override;

  std::vector<entry_t> table;
  mr_t table_mr;

  friend std::ostream &operator<<(std::ostream &s, const routing_table &t);

public:
  static const size_t predecessor_index = 0;
  static const size_t self_index = 1;
  static const size_t successor_index = 2;

  routing_table(RDMAServerSocket &root, const std::string &ip,
                const std::string &port);
};

class chord : public virtual RDMAClientSocket, public network {
public:
  chord(const std::string &host, const std::string &port);
  chord(const std::string &host, const std::string &port, const uint64_t,
        const size_t, const uint32_t);
  ~chord();
  node_id predecessor_node(const keyspace_t &id) const;
  node_id successor_node(const keyspace_t &id) const;
  node_id self() const;

private:
  passive &successor(const keyspace_t &id) override;

  std::vector<network::node> cache;
  routing_table load_table() const;
  routing_table find_table(const keyspace_t &) const;
  std::unique_ptr<RDMAObj<routing_table> > table;
  mr_t local_table_mr;
  mr table_mr;
};

std::ostream &operator<<(std::ostream &s, const routing_table &t);
kj::Array<capnp::word> predecessor_message(const node_id &);
kj::Array<capnp::word> update_message(const node_id &, const size_t &);
kj::Array<capnp::word>
chord_response(const rdma_ptr<LocalRDMAObj<routing_table> > &);

}
}
}

