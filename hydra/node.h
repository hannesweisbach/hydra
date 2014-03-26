#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "util/uint128.h"
#include "rdma/RDMAWrapper.hpp"
#include "rdma/RDMAServerSocket.h"
#include "rdma/RDMAClientSocket.h"
#include "hydra/hopscotch-server.h"
#include "hydra/messages.h"
#include "util/concurrent.h"
#include "util/WorkerThread.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"
#include "RDMAObj.h"

namespace hydra {
class node {
  struct rnode {
    __uint128_t start;
    __uint128_t end;
    RDMAClientSocket node;
  };
  RDMAServerSocket socket;

  ThreadSafeHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<hydra::rdma::REMOTE_READ>, 256> >,
      ZoneHeap<RdmaHeap<hydra::rdma::REMOTE_READ>, 256> > > heap;
  ThreadSafeHeap<ZoneHeap<RdmaHeap<hydra::rdma::LOCAL_READ>, 256> > local_heap;
  decltype(heap.malloc<key_entry>()) table_ptr;
  monitor<hopscotch_server> dht;
  monitor<std::unordered_map<qp_t, RDMAServerSocket::client_t> > clients;
  monitor<std::vector<rnode>> nodes;

  decltype(local_heap.malloc<request>()) msg_buffer;
  /* occupy threads for blocking work, so libdispatch doesn't choke */
  WorkerThread messageThread;
  WorkerThread acceptThread;

  monitor<decltype(heap.malloc<LocalRDMAObj<node_info>>())> info;

  void post_recv(request& m);
  void accept();
  void post_recv(const request& m, const ibv_mr* mr);
  void recv(const request &msg, const qp_t &qp);
  void send(const uint64_t id);
  void ack(const qp_t &qp, const response &msg) const;

  void handle_add(const put_request &msg, const qp_t &qp);
  void handle_del(const remove_request &msg, const qp_t &qp);
  std::future<void> notify_all(const msg& m);
  std::future<rdma_cm_id *> find_id(const qp_t &qp) const;

public:
  node(const std::string& ip, const std::string &port,
       uint32_t msg_buffers = 5);
  void connect(const std::string& host, const std::string& ip);
};

}

