#pragma once

#include <string>
#include <memory>
#include <vector>

#include "rdma/RDMAClientSocket.h"
#include "hydra/protocol/message.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"

namespace hydra {
class passive : public virtual RDMAClientSocket {
public:
  passive(const std::string &host, const std::string &port);

  bool put(const std::vector<unsigned char> &kv, const size_t &key_size);
  bool remove(const std::vector<unsigned char> &key);
  bool contains(const std::vector<unsigned char> &key);
  std::vector<unsigned char> get(const std::vector<unsigned char> &key);

  size_t table_size();

private:
  void init();
  void update_info();
  std::vector<unsigned char> find_entry(const std::vector<unsigned char> &key);

  using buffer_t =
      kj::FixedArray<capnp::word, ((256 + 40) / sizeof(capnp::word) + 1)>;
  std::unique_ptr<buffer_t> buffer;
  mr_t buffer_mr;

  SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 16 * 1024 * 1024> >,
      ZoneHeap<RdmaHeap<ibv_access::READ>, 128 * 1024 * 1024> > heap;

  std::unique_ptr<hydra::node_info> info;
  mr_t info_mr;

  using response_t = kj::FixedArray<capnp::word, 9>;
  std::unique_ptr<response_t> response;
  mr_t response_mr;

  mr remote;
};
}
