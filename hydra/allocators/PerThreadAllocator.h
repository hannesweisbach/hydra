#pragma once

#include <vector>
#include <thread>
#include <functional>

#include "util/Logger.h"
#include "hydra/hash.h"

#ifndef LEVEL1_DCACHE_LINESIZE
#error Set -DLEVEL1_DCACHE_LINESIZE=... as preprocessor define
#endif

namespace hydra {
template <typename SuperHeap> class PerThreadHeap {
  /* TODO get cache line size */
  struct alignas(LEVEL1_DCACHE_LINESIZE) AlignedHeap : SuperHeap {
    template <typename... Args>
    AlignedHeap(Args &&... args)
        : SuperHeap(std::forward<Args>(args)...) {}
  };
  AlignedHeap *heaps = nullptr;
  const size_t numHeaps;
#ifndef NDEBUG
  std::vector<size_t> usage;
#endif

public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;
  template <typename... Args>
  PerThreadHeap(size_t numHeaps, Args &&... args)
      : numHeaps(numHeaps)
#ifndef NDEBUG
        ,
        usage(numHeaps, 0)
#endif
  {
    heaps = reinterpret_cast<AlignedHeap *>(
        new char[sizeof(AlignedHeap) * numHeaps]);
    for (size_t i = 0; i < numHeaps; i++) {
      // TODO warn on rvalues
      new (&heaps[i]) AlignedHeap(std::forward<Args>(args)...);
    }
  }
#ifndef NDEBUG
  ~PerThreadHeap() {
    delete []heaps;
    log_info() << "Usage stats:";
    for(size_t i = 0; i < usage.size(); i++)
      log_info() << "Heap " << i << " " << usage[i];
  }
#endif

  template <typename T> rdma_ptr<T> malloc(const size_t n_elems = 1) {
#if 0
    std::hash<std::thread::id> hash;
    size_t index = hash(std::this_thread::get_id()) % heaps.size();
#else
    /* cityhash is faster and more uniform */
    size_t index = hydra::hash64(std::this_thread::get_id()) % numHeaps;
#endif
#ifndef NDEBUG
    usage[index]++;
#endif
    return heaps[index].template malloc<T>(n_elems);
  }
};
}
