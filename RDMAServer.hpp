#pragma once

class RDMAServer {
  struct rdma_event_channel * ec = nullptr;
  struct rdma_cm_id         * id = nullptr;
  struct rdma_cm_event      * ev = nullptr;
  
  public:
    RDMAServer();
    RDMAServer(const uint16_t port);
    RDMAServer(const std::string& host, const uint16_t port);
    ~RDMAServer();
};

