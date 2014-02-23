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
  
class RDMAClientSocket {
  std::shared_ptr< ::rdma_cm_id> id;
  std::future<void> fut_recv;
  std::future<void> fut_send;
#ifdef HAVE_LIBDISPATCH
  dispatch_queue_t send_queue;
  dispatch_queue_t recv_queue;
#endif
  std::atomic_bool running;
public:
  RDMAClientSocket(const std::string &host, const std::string &port);
  ~RDMAClientSocket();
  void connect();
  mr_ptr register_remote_read(void *ptr, size_t size) const;
  mr_ptr register_local_read(void *ptr, size_t size) const;
  
  template <typename T>
  [[deprecated]] std::future<T> recv_async(T &local,
                               std::shared_ptr<ibv_mr> &mr) {
    log_info() << "aync recv";
    return rdma_recv_async(id.get(), local, mr);
  }

  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  std::future<T *> recv_async(const T &local, const ibv_mr *mr,
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

  template <typename T> void sendImmediate(const T &o) const {
    log_info() << "size: " << sizeof(T);
    log_hexdump(o);
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
  std::future<T *> read(T *local, ibv_mr *mr, U *remote, uint32_t rkey,
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
