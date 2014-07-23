#pragma once

#include <string>

#include "rdma/RDMAClientSocket.h"
#include "hydra/types.h"

namespace hydra {
namespace chord {
node_id predecessor(const routing_table &start, const keyspace_t &id);
node_id successor(const routing_table &start, const keyspace_t &id);

class node : public virtual RDMAClientSocket {
public:
  node(const std::string &host, const std::string &port);
  node_id predecessor(const keyspace_t &id) const;
  node_id successor(const keyspace_t &id) const;
  node_id self() const;

private:
  hydra::routing_table load_table() const;
  hydra::routing_table find_table(const keyspace_t &) const;
  mr table_mr;
};
}
}

