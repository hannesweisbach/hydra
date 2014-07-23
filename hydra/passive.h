#pragma once

#include <string>
#include <atomic>
#include <memory>
#include <functional>
#include <vector>

#include "rdma/RDMAWrapper.hpp"
#include "rdma/RDMAClientSocket.h"
#include "types.h"
#include "hydra/protocol/message.h"

#include "util/WorkerThread.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"

namespace hydra {
class passive : public virtual RDMAClientSocket {
public:
  passive(const std::string &host, const std::string &port);

#if 0
  routing_entry predecessor(const __uint128_t &id) const;
  node_id successor(const __uint128_t &id) const;
#endif
  void update_predecessor(const hydra::node_id &pred) const;
  bool has_id(const keyspace_t &id) const;

  bool put(const std::vector<unsigned char> &kv, const size_t &key_size) const;
  bool remove(const std::vector<unsigned char> &key) const;
  bool contains(const std::vector<unsigned char> &key) const;
  std::vector<unsigned char> get(const std::vector<unsigned char> &key) const;

private:
  void update_info();
  std::vector<unsigned char>
  find_entry(const std::vector<unsigned char> &key) const;

  using buffer_t =
      kj::FixedArray<capnp::word, ((256 + 40) / sizeof(capnp::word) + 1)>;
  std::unique_ptr<buffer_t> buffer;
  mr_t buffer_mr;

  ThreadSafeHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 256> >,
      ZoneHeap<RdmaHeap<ibv_access::READ>, 256> > > heap;
  ThreadSafeHeap<ZoneHeap<RdmaHeap<ibv_access::MSG>, 256> > local_heap;
  decltype(local_heap.malloc<node_info>()) info;

  mr remote;
};
}
