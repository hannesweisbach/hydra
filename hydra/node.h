#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "util/uint128.h"
#include "rdma/RDMAWrapper.hpp"
#include "rdma/RDMAServerSocket.h"
#include "rdma/RDMAClientSocket.h"
#include "hydra/hopscotch-server.h"
#include "hydra/types.h"
#include "hydra/chord.h"
#include "protocol/message.h"

#include "util/concurrent.h"
#include "util/WorkerThread.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"
#include "RDMAObj.h"

#include "passive.h"

namespace hydra {
class node {
  RDMAServerSocket socket;

  mutable ThreadSafeHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 256> >,
      ZoneHeap<RdmaHeap<ibv_access::READ>, 256> > > heap;
  mutable ThreadSafeHeap<ZoneHeap<RdmaHeap<ibv_access::MSG>, 256> > local_heap;
  decltype(heap.malloc<LocalRDMAObj<hash_table_entry> >()) table_ptr;
#if PER_ENTRY_LOCKS
  hopscotch_server dht;
#else
  monitor<hopscotch_server> dht;
#endif

  using request_t = kj::FixedArray<capnp::word, 9>;
  std::vector<request_t> request_buffers;
  mr_t buffers_mr;

  /* occupy threads for blocking work, so libdispatch doesn't choke */
  WorkerThread messageThread;

  monitor<decltype(heap.malloc<LocalRDMAObj<node_info>>())> info;
  decltype(heap.malloc<LocalRDMAObj<routing_table>>()) routing_table_;

  void post_recv(const request_t &);
  void recv(const request_t &, const qp_t &qp);
  void send(const uint64_t id);
  void reply(const qp_t &qp, ::capnp::MessageBuilder &reply) const;
  void reply(const qp_t &qp, const ::kj::Array< ::capnp::word> &reply) const;

  bool handle_add(rdma_ptr<unsigned char> kv, const size_t size,
                  const size_t key_size);
  void handle_add(const protocol::DHTRequest::Put::Inline::Reader &reader,
                  const qp_t &qp);
  void handle_add(const protocol::DHTRequest::Put::Remote::Reader &reader,
                  const qp_t &);
  void handle_del(const protocol::DHTRequest::Del::Remote::Reader &reader,
                  const qp_t &qp) const;
  void handle_del(const protocol::DHTRequest::Del::Inline::Reader &reader,
                  const qp_t &qp) const;

  /* call when joining the network - already running node ip */
  void init_routing_table(const hydra::passive& remote);
  void update_others() const;
  void update_routing_table(const hydra::node_id &e, const size_t i);

public:
  node(const std::string& ip, const std::string &port,
       uint32_t msg_buffers = 5);
  void join(const std::string& ip, const std::string& port);
  double load() const;
  void dump() const;
};

}

