#pragma once

#include <memory>
#include <functional>

#include <sys/mman.h>
#include <rdma/rdma_verbs.h>

#include "util/utils.h"
#include "allocators/config.h"

#include "rdma/RDMAWrapper.hpp"

template <ibv_access access = ibv_access::READ> class RdmaHeap {
public:
  enum {
    Alignment = 4096,
    Shift = hydra::util::static_log2<Alignment>::value
  };

  template <typename T> using pointer_t = pointer_t<T>;
  template <typename T> using rdma_ptr = rdma_ptr<T>;

  template <typename T>
  RdmaHeap(T &rdma_instance)
      : self_(new rdma_allocator_model<T>(rdma_instance)) {
    static_assert(is_power_of_two<Alignment>::value,
                  "Alignment must be power of two");
  }

  /* Maybe do something like:
   * template <typename T>
   * alloc_type malloc(size_t size, const T& registrar) {...}
   * but then, the registrar has to be dragged to all heap layers...
   */
  template <typename T> rdma_ptr<T> malloc(size_t size = sizeof(T)) {
    T *ptr = reinterpret_cast<T *>(mmap(nullptr, size, PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (ptr == MAP_FAILED)
      throw std::bad_alloc();
    try {
      auto mr = self_->register_memory(access, ptr, size);
      /* retain pointer before moving the unique_ptr into the deleter lambda
       * edit: we cant reset the pointer in the lambda because it may be mutable
       * thus call rdma_dereg_mr in the lambda on the raw-pointer and release
       * the managed pointer here.
       */
      ibv_mr *mr_ = mr.get();
      auto ret = rdma_ptr<T>(pointer_t<T>(ptr, [=](void *p) {
                               // mr.reset();
                               rdma_dereg_mr(mr_);
                               ::munmap(p, size);
                             }),
                             mr_);
      mr.release();
      return ret;
    }
    catch (const std::exception &e) {
      ::munmap(ptr, size);
      std::cerr << "foo " << e.what() << std::endl;
      throw;
    }
  }

private:
  struct rdma_allocator_concept {
    virtual ~rdma_allocator_concept() = default;
    virtual mr_t register_memory(const ibv_access &flags, void *ptr,
                                 size_t size) const = 0;
  };

  template <typename T> struct rdma_allocator_model : rdma_allocator_concept {
    T &data_;
    rdma_allocator_model(T &data) : data_(data) {}
    mr_t register_memory(const ibv_access &flags, void *ptr, size_t size) const
        override {
      return hydra::register_memory(data_, flags, ptr, size);
    }
  };

  std::shared_ptr<const rdma_allocator_concept> self_;
};

