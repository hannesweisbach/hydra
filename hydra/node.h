#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>

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
  monitor<std::vector<RDMAServerSocket::client_t>> clients; 
  monitor<std::vector<rnode>> nodes;

  decltype(local_heap.malloc<msg>()) msg_buffer;
  /* occupy threads for blocking work, so libdispatch doesn't choke */
  WorkerThread messageThread;
  WorkerThread acceptThread;

  std::mutex resize_mutex;

  decltype(heap.malloc<node_info>()) info;

  void post_recv(msg& m);
  void accept();
  void post_recv(const msg& m, const ibv_mr* mr);
  void recv(const msg& msg);
  void send(const uint64_t id);
  template <typename T, typename = typename std::enable_if<
                            std::is_same<T, msg_add>::value ||
                            std::is_same<T, msg_del>::value>::type>
  void ack(const T &msg) const {
    msg_ack m(msg);
    rdma_cm_id *id = reinterpret_cast<rdma_cm_id *>(msg.id());
    log_info() << m;
    sendImmediate(id, m);
  }


  void handle_add(const msg_add& msg);
  void handle_del(const msg_del& msg);
  std::future<void> notify_all(const msg& m);
public:
  node(const std::string& ip, const std::string &port,
       uint32_t msg_buffers = 5);
  void connect(const std::string& host, const std::string& ip);
};

}

