#pragma once

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>

#include "RDMAWrapper.hpp"
#include "util/WorkerThread.h"

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#endif

class RDMAServerSocket {
  rdma_id_ptr id;
  comp_channel_ptr cc;
  cq_ptr cq;
  WorkerThread acceptThread;
  std::future<void> async_fut;
  std::atomic_bool running;

public:
  using client_t =
      std::unique_ptr<rdma_cm_id, std::function<void(rdma_cm_id *)> >;

  RDMAServerSocket(const std::string &host, const std::string &port,
                   uint32_t max_wr = 10, int cq_entries = 10);
  ~RDMAServerSocket();
  /* TODO: don't need as member fn */
  void sendImmediate(rdma_cm_id *id, const void *buffer, const size_t size) const;

  void listen(int backlog = 10);
  std::future<client_t> accept();
  
  template <typename T>
  std::future<T *> recv_async(const T *local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), local, mr, size);
  }

  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  std::future<T *> recv_async(const T &local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), &local, mr, size);
  }

  template <typename T>
  [[deprecated]]  ibv_mr * regMemory(const T*ptr, size_t size = sizeof(T)) const {
      log_trace() << id.get() << " " << (void*) this;
      return check_nonnull(rdma_reg_read(id.get(), static_cast<void*>(const_cast<T*>(ptr)), size), "rdma_reg_read");
  }

  mr_ptr register_remote_read(void * ptr, size_t size) const;
  mr_ptr register_local_read(void * ptr, size_t size) const;
};

namespace hydra {
  inline mr_ptr register_remote_read(RDMAServerSocket& socket, void * ptr, size_t size) {
    return socket.register_remote_read(ptr, size);
  }
  
  inline mr_ptr register_local_read(RDMAServerSocket& socket, void * ptr, size_t size) {
    return socket.register_local_read(ptr, size);
  }
}
