#pragma once

#include <string>
#include <memory>

#include <rdma/rdma_cma.h>

class RDMAClient {
  std::shared_ptr< ::rdma_cm_id> id;
  public:
    RDMAClient(const std::string& host, const std::string& port);

    void run();
};
