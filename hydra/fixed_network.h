#pragma once

#include <vector>
#include <string>
#include <cstdint>

#include "hydra/network.h"
#include "rdma/RDMAClientSocket.h"
#include "rdma/RDMAServerSocket.h"

#include "hydra/RDMAObj.h"

namespace hydra {
namespace overlay {
namespace fixed {

class fixed : public network {
  std::vector<node> nodes;
  passive &successor(const keyspace_t &id) override;

public:
  fixed(RDMAClientSocket &, uint64_t, const uint32_t, const uint16_t);
};

class routing_table : public hydra::overlay::routing_table {
  std::vector<entry_t> table;
  mr_t table_mr;

  kj::Array<capnp::word> init() const override;
  kj::Array<capnp::word> process_join(const std::string &host,
                                      const std::string &port) override;
  void update(const std::string &host, const std::string &port,
              const keyspace_t &id, const size_t index) override;

public:
  routing_table(RDMAServerSocket &socket, const std::string &host,
                const std::string &port, uint16_t size);
};

kj::Array<capnp::word>
network_response(const rdma_ptr<LocalRDMAObj<routing_table> > &,
                 const uint16_t);
}
}
}
