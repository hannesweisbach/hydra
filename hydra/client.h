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
auto size2Class = [](size_t size) -> size_t {
  if (size == 0)
    return 0;
  if (size <= 128) // 16 bins
    return ((size + (8 - 1)) & ~(8 - 1)) / 8 - 1;
  else if (size <= 4096) // 31 bins
    return ((size + (128 - 1)) & ~(128 - 1)) / 128 + 14;
  else
    return 47 + hydra::util::log2(size - 1) -
           hydra::util::static_log2<4096>::value;
};

class client {
public:
  client(const std::string &host, const std::string &port);
  ~client();
  std::future<void>
  post_recv(const msg& msg, const ibv_mr* mr);
  void recv(const msg& msg);
  std::future<bool> add(const char *key, size_t key_length, const char *value,
           size_t value_length);
  std::future<bool> remove(const char * key, size_t key_length);
  bool contains(const char * key, size_t key_length);
  typedef std::unique_ptr<char, std::function<void(char*)>> value_ptr;
  value_ptr get(const char * key, size_t key_length);
  size_t size() const { return table_size; } 
private:
  RDMAClientSocket s;
  ThreadSafeHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<hydra::rdma::REMOTE_READ>, 256> >,
      ZoneHeap<RdmaHeap<hydra::rdma::REMOTE_READ>, 256> > > heap;
  ThreadSafeHeap<ZoneHeap<RdmaHeap<hydra::rdma::LOCAL_READ>, 256>> local_heap;
  decltype(local_heap.malloc<msg>()) msg_buffer;
  decltype(local_heap.malloc<node_info>()) info;

  WorkerThread messageThread;
  
  mr remote;

  key_entry * remote_table;
  //TODO: node for hash
  size_t prefetch; 

  uint64_t id;
  size_t table_size;
  uint32_t rkey;

};
}
