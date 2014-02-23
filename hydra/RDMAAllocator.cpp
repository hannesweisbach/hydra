#include "node.h"
#include "RDMAAllocator.h"

#if 0
RDMAAllocator * rdma_alloc = nullptr;
SysAllocator * tc_get_sysalloc_override(SysAllocator * def) {
  (void)(def);
  return rdma_alloc;
}

namespace hydra {
std::shared_ptr<ibv_mr> register_read(size_t &s, void *ptr, size_t size) {
  (void)(s);
  (void)(ptr);
  (void)(size);
  return std::shared_ptr<ibv_mr>(nullptr);
}
}
#endif

