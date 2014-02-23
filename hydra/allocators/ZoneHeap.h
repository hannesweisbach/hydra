#pragma once

#include <utility>
#include <cstddef>
#include <vector>
#include <algorithm>

#include "allocators/config.h"

namespace hydra {
template <typename SuperHeap, size_t chunkSize>
class ZoneHeap : public SuperHeap {
public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;
  template <typename... Args>
  ZoneHeap(Args &&... args)
      : SuperHeap(std::forward<Args>(args)...), remaining(0), current(0) {
    static_assert(chunkSize >= hydra::AllocatorConfig::Alignment,
                  "chunkSize needs to be at least as large as the alignment "
                  "requirement.");
  }

  template <typename T> inline rdma_ptr<T> malloc(const size_t n_elems = 1) {
    size_t size =
        hydra::align<hydra::AllocatorConfig::Alignment>(n_elems * sizeof(T));
    if (remaining < size)
      expand(std::max(size, chunkSize));

    remaining -= size;
    rdma_ptr<T> ret(
        pointer_t<T>(reinterpret_cast<T *>(arenas.back().first.get() + current),
                     [](T *p) { /*log_debug() << "Deallocating " << p;*/ }),
        arenas.back().second);
    current += size;

    return ret;
  }

private:
  void expand(size_t size = chunkSize) {
    arenas.push_back(SuperHeap::template malloc<char>(size));
    remaining = size;
    current = 0;
  }
  std::vector<rdma_ptr<char> > arenas;
  size_t remaining;
  size_t current;
};
}
