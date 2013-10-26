#pragma once

#include <string>

class RDMAServer {
  struct rdma_event_channel * ec = nullptr;
  struct rdma_cm_id         * id = nullptr;
  
  public:
    RDMAServer();
    RDMAServer(const std::string& port);
    RDMAServer(const std::string& host, const std::string& port);
    ~RDMAServer();
};

