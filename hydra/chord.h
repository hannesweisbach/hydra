#pragma once

#include <string>

#include "rdma/RDMAClientSocket.h"
#include "hydra/types.h"

namespace hydra {
namespace chord {
class node : public virtual RDMAClientSocket {
public:
  node(const std::string &host, const std::string &port);
  node_id predecessor(const keyspace_t &id) const;
  node_id successor(const keyspace_t &id) const;
  node_id self() const;

private:
  hydra::routing_table load_table() const;
  hydra::routing_table find_table(const keyspace_t &) const;
  RDMAObj<hydra::routing_table> table;
  mr_t local_table_mr;
  mr table_mr;
};
}
}

