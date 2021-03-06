#pragma once

#include <mutex>

#include "util/concurrent.h"

namespace hydra {
template <typename SuperHeap> class LockedHeap {
  SuperHeap superheap;
  spinlock lock;

public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;
  template <typename... Args>
  LockedHeap(Args &&... args)
      : superheap(std::forward<Args>(args)...) {}
  template <typename T> rdma_ptr<T> malloc(const size_t n_elems = 1) {
    std::unique_lock<spinlock> l(lock);
    return superheap.template malloc<T>(n_elems);
  }
};
}
