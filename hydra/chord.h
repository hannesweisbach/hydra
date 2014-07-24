#pragma once

#include <string>
#include <memory>
#include <vector>

#include "rdma/RDMAClientSocket.h"
#include "hydra/types.h"
#include "hydra/network.h"

namespace hydra {
namespace chord {
class node : public virtual RDMAClientSocket, public network {
public:
  node(const std::string &host, const std::string &port);
  node(const std::string &host, const std::string &port, const uint64_t,
       const size_t, const uint32_t);
  ~node();
  node_id predecessor_node(const keyspace_t &id) const;
  node_id successor_node(const keyspace_t &id) const;
  node_id self() const;

private:
  passive &successor(const keyspace_t &id) override;

  std::vector<network::node> cache;
  hydra::routing_table load_table() const;
  hydra::routing_table find_table(const keyspace_t &) const;
  std::unique_ptr<RDMAObj<hydra::routing_table> > table;
  mr_t local_table_mr;
  mr table_mr;
};
}
}

