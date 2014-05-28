#pragma once
#include <utility>
#include <cstddef>
#include "util/concurrent.h"

namespace hydra {
template <typename SuperHeap> class ThreadSafeHeap {
  monitor<SuperHeap> superHeap;

public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;
  template <typename... Args>
  ThreadSafeHeap(Args &&... args)
      : superHeap(std::forward<Args>(args)...) {}
  ThreadSafeHeap(ThreadSafeHeap &&other)
      : superHeap(std::move(other.superHeap)) {}

  ThreadSafeHeap &operator=(ThreadSafeHeap &&other) {
    std::swap(superHeap, other.superHeap);
    return *this;
  }

  template <typename T> inline rdma_ptr<T> malloc(const size_t n_elems = 1) {
    return superHeap([=](SuperHeap &s) {
                       return s.template malloc<T>(n_elems);
                     }).get();
  }
};
}
