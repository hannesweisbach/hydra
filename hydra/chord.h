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
  chord(const std::string &, const std::string &);
  chord(const std::string &host, const std::string &port, const uint64_t,
        const uint32_t, const uint16_t);
  ~chord();
  node_id predecessor_node(const keyspace_t &id);
  node_id successor_node(const keyspace_t &id);
  node_id self();

private:
  passive &successor(const keyspace_t &id) override;

  std::vector<network::node> cache;
  std::vector<entry_t> load_table();
  std::vector<entry_t> find_table(const keyspace_t &);
  std::vector<entry_t> local_table;
  mr_t local_table_mr;
  mr table_mr;
};

std::ostream &operator<<(std::ostream &s, const routing_table &t);

}
}
}

