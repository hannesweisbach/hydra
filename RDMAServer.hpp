#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <rdma/rdma_cma.h>

#include "RDMAWrapper.hpp"

class RDMAServer {
  std::shared_ptr< ::rdma_event_channel> ec;
  std::shared_ptr< ::rdma_cm_id> id;
  std::shared_ptr< ::ibv_comp_channel> cc;
  std::shared_ptr< ::ibv_cq> cq;
  //std::vector<std::shared_ptr< ::ibv_pd>> pds;
  // std::shared_ptr<::ibv_comp_channel> channel;

  //std::shared_ptr< ::ibv_context> context;

  struct client {
    std::shared_ptr<rdma_cm_id> id;
    std::vector<std::shared_ptr<ibv_mr>> mrs;
    client(std::shared_ptr<rdma_cm_id> id) : id(id) {}
  };

  std::vector<std::shared_ptr<rdma_cm_id>> clients;
  std::vector<RDMAReadMem> mem;

public:
  RDMAServer();
  RDMAServer(const std::string &port);
  RDMAServer(const std::string &host, const std::string &port);


  void * allocMapMemory(size_t size, size_t alignment = 4096);
  void run();
  void processEvents();
  void post_send(rdma_cm_id *);
};

