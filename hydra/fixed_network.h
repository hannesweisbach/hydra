#pragma once

#include <vector>

#include "hydra/network.h"
#include "rdma/RDMAClientSocket.h"
#include "rdma/RDMAServerSocket.h"

#include "hydra/RDMAObj.h"

namespace hydra {
namespace overlay {
namespace fixed {

using entry_t = LocalRDMAObj<routing_entry>;

class fixed : public network {
  std::vector<node> nodes;
  passive &successor(const keyspace_t &id) override;

public:
  fixed(RDMAClientSocket &, const uint64_t, const size_t, const uint32_t);
};

class routing_table : public hydra::overlay::routing_table {
  std::vector<entry_t> table;
  mr_t table_mr;

  kj::Array<capnp::word> init() const override;

public:
  routing_table(RDMAServerSocket &socket, const size_t size)
      : table(size), table_mr(socket.register_memory(ibv_access::READ, table)) {
    // allocate / register memory
    // fill table
  }
};

kj::Array<capnp::word>
network_response(const rdma_ptr<LocalRDMAObj<routing_table> > &,
                 const uint16_t);
}
}
}
