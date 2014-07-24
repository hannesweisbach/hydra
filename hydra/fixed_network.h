#pragma once

#include <vector>

#include "hydra/network.h"
#include "rdma/RDMAClientSocket.h"

namespace hydra {

class fixed : public network {
  std::vector<node> nodes;
  passive &successor(const keyspace_t &id) override;

public:
  fixed(RDMAClientSocket &, const uint64_t, const size_t, const uint32_t);
};
}
