#pragma once

#include <string>
#include <atomic>
#include <memory>
#include <functional>

#include "rdma/RDMAWrapper.hpp"
#include "rdma/RDMAClientSocket.h"
#include "types.h"
#include "messages.h"

#include "util/WorkerThread.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"

namespace hydra {
class passive {
public:
  passive(const std::string &host, const std::string &port);
  passive(passive &&);
  passive &operator=(passive &&);
  std::future<void> post_recv(const msg &msg, const ibv_mr *mr);
  void recv(const msg& msg);

#if 0
  routing_entry predecessor(const __uint128_t &id) const;
  node_id successor(const __uint128_t &id) const;
#endif
  routing_table table() const;
  void update_predecessor(const hydra::node_id &pred) const;
  bool has_id(const keyspace_t &id) const;
  void send(const msg&) const;

private:

  void update_info();
  RDMAClientSocket s;

  ThreadSafeHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 256> >,
      ZoneHeap<RdmaHeap<ibv_access::READ>, 256> > > heap;
  ThreadSafeHeap<ZoneHeap<RdmaHeap<ibv_access::MSG>, 256> > local_heap;
  decltype(local_heap.malloc<msg>()) msg_buffer;
  decltype(local_heap.malloc<node_info>()) info;

  WorkerThread messageThread;

  mr remote;
};
}
