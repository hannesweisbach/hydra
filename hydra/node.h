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
#include "hydra/server_dht.h"
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
  std::unique_ptr<server_dht> dht;
#else
  monitor<std::unique_ptr<server_dht>> dht;
#endif

  using request_t = kj::FixedArray<capnp::word, 128>;
  std::vector<request_t> request_buffers;
  mr_t buffers_mr;

  /* occupy threads for blocking work, so libdispatch doesn't choke */
  WorkerThread messageThread;

  monitor<decltype(heap.malloc<LocalRDMAObj<node_info>>())> info;
  std::unique_ptr<hydra::overlay::routing_table> routing_table;

  std::string ip;
  std::string port;
  keyspace_t start;
  keyspace_t end;

  using response_t = kj::Array<capnp::word>;
  response_t ack;
  response_t nack;

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

public:
  node(std::vector<std::string> ips, const std::string &port,
       size_t initial_size = 1024 * 1024, uint32_t msg_buffers = 1024);
  void join(const std::string& ip, const std::string& port);
  double load() const;
  size_t size() const;
  size_t used() const;
  void dump() const;
};

}

