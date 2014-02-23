#pragma once

#include <vector>
#include <memory>
#include <functional>

#include <rdma/rdma_verbs.h>
#include "Logger.h"

#if 0
#include <gperftools/malloc_extension.h>
#include <gperftools/tcmalloc.h>
#include <tcmalloc_guard.h>


/* This is RDMA-TCMalloc glue */
class RDMAAllocator : public SysAllocator {

  struct rdma_allocator_concept {
    virtual ~rdma_allocator_concept() = default;
    virtual void *allocateMapMemory(size_t size, size_t alignment) const = 0;
    virtual void mapMemory() const = 0;
  };

  template <typename T> struct rdma_allocator_model : rdma_allocator_concept {
    T &data_;
    rdma_allocator_model(T &data) : data_(data) {}
    void *allocateMapMemory(size_t size, size_t alignment) const override {
      return ::allocateMapMemory(data_, size, alignment);
    }
    void mapMemory() const override {
      // return ::mapMemory(data_);
    }
  };
  std::shared_ptr<const rdma_allocator_concept> self_;

public:
  template <typename T>
  RDMAAllocator(T &rdma_instance)
      : self_(new rdma_allocator_model<T>(rdma_instance)) {
    MallocExtension::instance()->SetSystemAllocator(this);
  }
  ~RDMAAllocator() override{};

  // Allocates "size"-byte of memory from system aligned with "alignment".
  // Returns NULL if failed. Otherwise, the returned pointer p up to and
  // including (p + actual_size -1) have been allocated.
  // TODO: dealloc?
  virtual void *Alloc(size_t size, size_t *actual_size,
                      size_t alignment) override {
    *actual_size = size;
    log_info() << "Allocated " << size << " bytes";
    return self_->allocateMapMemory(size, alignment);
    // return (void*)mrs.back()->addr;
  }
};

extern RDMAAllocator *rdma_alloc;
SysAllocator *tc_get_sysalloc_override(SysAllocator *def);
#endif

#include <unordered_map>

#include <sys/mman.h>
#include "util/exception.h"
#include "util/concurrent.h"

#include "allocators/config.h"

namespace hydra {
class node;
}
extern hydra::node *allocation_node;

namespace hydra {
namespace rdma {
enum Access {
  LOCAL_READ,
  REMOTE_READ
};
}
}

template <hydra::rdma::Access access = hydra::rdma::LOCAL_READ> class RdmaHeap {
public:
  enum {
    Alignment = 4096,
    Shift = hydra::util::static_log2<Alignment>::value
  };
  template <typename T>
  using pointer_t = std::unique_ptr<T, std::function<void(T *)> >;
  template <typename T> using rdma_ptr = std::pair<pointer_t<T>, ibv_mr *>;

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
  template <typename T>
  rdma_ptr<T> malloc(size_t size = sizeof(T)) 
  {
// size = (size + CPUInfo::PageSize - 1) & ~(CPUInfo::PageSize - 1);
#if 0
    auto ptr = memory_type([=]() {
                             void *ptr =
                                 mmap(nullptr, size, PROT_READ | PROT_WRITE,
                                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                             if (ptr == MAP_FAILED)
                               throw std::bad_alloc();

                             return ptr;
                           }(),
                           std::bind(::munmap, std::placeholders::_1, size));
    switch (access) {
    case hydra::rdma::REMOTE_READ:
      return alloc_type(
          std::move(ptr),
          std::move(mr_type(self_->register_remote_read(ptr.get(), size))));
    case hydra::rdma::LOCAL_READ:
      return alloc_type(
          std::move(ptr),
          std::move(mr_type(self_->register_local_read(ptr.get(), size))));
    }
#else
    T *ptr = reinterpret_cast<T *>(
        mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
             -1, 0));
    if (ptr == MAP_FAILED)
      throw std::bad_alloc();
    try {
      auto mr = self_->register_remote_read(ptr, size);
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
    catch (std::exception &) {
      ::munmap(ptr, size);
      throw std::current_exception;
    }
#endif
#if 0
  assert(reinterpret_cast<size_t>(ptr) % Alignment == 0);
#endif
  }
#if 0
  ibv_mr *getMr(void *ptr) const {
    void *p = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(ptr) &
                                       ~(Alignment - 1));
    return metadata_map([=](map_type &map) {
                          const auto it = map.find(p);
                          if (it == map.end()) {
                            std::ostringstream s("Invalid getMr on pointer ");
                            s << ptr;
                            throw std::runtime_error(s.str());
                          }
                          return it->second.second.get();
                        }).get();
  }
#endif
private:
  struct rdma_allocator_concept {
    virtual ~rdma_allocator_concept() = default;
    virtual mr_ptr register_remote_read(void *ptr, size_t size) const = 0;
    virtual mr_ptr register_local_read(void *ptr, size_t size) const = 0;
  };

  template <typename T> struct rdma_allocator_model : rdma_allocator_concept {
    T &data_;
    rdma_allocator_model(T &data) : data_(data) {}
    mr_ptr register_remote_read(void *ptr, size_t size) const override {
      return hydra::register_remote_read(data_, ptr, size);
    }
    mr_ptr register_local_read(void *ptr, size_t size) const override {
      return hydra::register_local_read(data_, ptr, size);
    }
  };

  std::shared_ptr<const rdma_allocator_concept> self_;
};

