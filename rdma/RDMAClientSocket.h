#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <typeinfo>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#endif

#include "RDMAWrapper.hpp"
#include "util/exception.h"
#include "util/Logger.h"
#include "hydra/types.h"

class RDMAClientSocket;

namespace hydra {
inline mr_ptr register_remote_read(RDMAClientSocket &socket, void *ptr,
                                   size_t size);
inline mr_ptr register_local_read(RDMAClientSocket &socket, void *ptr,
                                  size_t size);
}

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "RDMAAllocator.h"
#include "hydra/RDMAObj.h"
  
class RDMAClientSocket {
  rdma_id_ptr id;
  std::future<void> fut_recv;
  std::future<void> fut_send;
#ifdef HAVE_LIBDISPATCH
  dispatch_queue_t send_queue;
  dispatch_queue_t recv_queue;
#endif
  std::atomic_bool running;

#if 0
  mutable hydra::ThreadSafeHeap<hydra::ZoneHeap<RdmaHeap<hydra::rdma::LOCAL_READ>, 256>> local_heap;
#else
  mutable hydra::ThreadSafeHeap<hydra::FreeListHeap<
      hydra::ZoneHeap<RdmaHeap<hydra::rdma::LOCAL_READ>, 1024> > > local_heap;
  mutable hydra::ThreadSafeHeap<hydra::ZoneHeap<
      RdmaHeap<hydra::rdma::REMOTE_READ>, 1024 * 1024> > remote_heap;
#endif

public:
  RDMAClientSocket(const std::string &host, const std::string &port);
  RDMAClientSocket(RDMAClientSocket &&);
  RDMAClientSocket &operator=(RDMAClientSocket &&);
  /* TODO: optimize. maybe it is better to make uint32_t/uint16_t from strings,
   * or to have two independent ctors
   */
  //RDMAClientSocket(const uint32_t host, const uint32_t port)
  //    : RDMAClientSocket(std::to_string(port)) {}
  ~RDMAClientSocket();
  void connect() const;
  void disconnect() const;
  mr_ptr register_remote_read(void *ptr, size_t size) const;
  mr_ptr register_local_read(void *ptr, size_t size) const;
  
  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  auto recv_async(const T &local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), &local, mr, size);
  }

  template <typename T>
  ibv_mr *mapMemory(const T *ptr, size_t size = sizeof(T)) const {
    return check_nonnull(
        rdma_reg_read(id.get(), static_cast<void *>(const_cast<T *>(ptr)),
                      size),
        "rdma_reg_read");
  }

  template <typename T> auto malloc(const size_t n_elems = 1) {
    return remote_heap.malloc<T>(n_elems);
  }

  template <typename T> void sendImmediate(const T &o) const {
    if (rdma_post_send(id.get(), nullptr,
                       static_cast<void *>(const_cast<T *>(&o)), sizeof(T),
                       nullptr, IBV_SEND_INLINE))
      log_error() << "rdma_post_send " << strerror(errno);
  }
  
  template <typename T> void sendImmediate(const T* o, size_t size) const {
    hexdump(o, size);
    if (rdma_post_send(id.get(), nullptr,
                       static_cast<void *>(const_cast<T *>(o)), size,
                       nullptr, IBV_SEND_INLINE))
      log_error() << "rdma_post_send " << strerror(errno);
  }

  template <typename T>
  void read(T *local, uint64_t remote, uint32_t rkey,
                           size_t size = sizeof(T)) {

    log_trace() << "Reading remote @" << std::hex << std::showbase << remote
                << std::dec << " (" << size << ") " << rkey << " into local "
                << (void *)local;
    rdma_read_async(id, local, remote, rkey, size).get();
  }

  template <typename T, typename U>
  auto read(T *local, ibv_mr *mr, U *remote, uint32_t rkey,
                        size_t n_elems = 1) {
    static_assert(std::is_same<typename std::remove_cv<T>::type,
                               typename std::remove_cv<U>::type>::value,
                  "Need same types.");
    log_debug() << "Reading from " << (void *)remote << " (" << rkey << ") to "
                << (void *)local << " " << mr << " (" << n_elems << " "
                << typeid(T).name() << ", " << n_elems * sizeof(T) << " bytes)";
    return rdma_read_async__(id.get(), local, n_elems * sizeof(T), mr,
                             reinterpret_cast<uint64_t>(remote), rkey);
  }

  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  auto recv_async() {
    auto buffer = local_heap.malloc<T>();
    auto future = rdma_recv_async(id, buffer);
    return std::make_pair(std::move(future), std::move(buffer));
  }

  template <typename T>
  auto read(uint64_t remote, uint32_t rkey, size_t size = sizeof(T)) const {
    assert(size == sizeof(T));
    auto buffer = local_heap.malloc<T>(size);
    auto future = rdma_read_async(id, buffer, size, remote, rkey);
    return std::make_pair(std::move(future), std::move(buffer));
  }

  template <typename T>
  auto from(const uint64_t addr, const uint32_t rkey, size_t retries = 0)
      -> decltype(local_heap.malloc<RDMAObj<T> >()) {
    auto o = local_heap.malloc<RDMAObj<T> >();
    reload(o, addr, rkey, retries);
    return o;
  }

  template <typename T>
  void reload(decltype(local_heap.malloc<RDMAObj<T> >()) & o,
              const uint64_t addr, const uint32_t rkey, size_t retries = 0) {
    do {
      read(o.first.get(), o.second, reinterpret_cast<T *>(addr), rkey).get();
    } while (retries-- > 0 && o.first->valid());

    if (!o.first->valid())
      throw std::runtime_error("Invalid remote object");
  }
};

std::shared_ptr<ibv_mr>
allocateMapMemory(const RDMAClientSocket &s, size_t size, size_t alignment);

namespace hydra {
  inline mr_ptr register_remote_read(RDMAClientSocket& socket, void * ptr, size_t size) {
    return socket.register_remote_read(ptr, size);
  }
  
  inline mr_ptr register_local_read(RDMAClientSocket& socket, void * ptr, size_t size) {
    return socket.register_local_read(ptr, size);
  }
}

