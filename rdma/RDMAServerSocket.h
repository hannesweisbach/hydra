#pragma once

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>

#include "RDMAWrapper.hpp"
#include "util/WorkerThread.h"
#include "util/concurrent.h"

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#endif

class RDMAServerSocket {
private:
  using client_t =
      std::unique_ptr<rdma_cm_id, std::function<void(rdma_cm_id *)> >;
  ec_ptr ec;
  rdma_id_ptr id;
  comp_channel_ptr cc;
  cq_ptr cq;
  WorkerThread eventThread;
  std::future<void> async_fut;
  std::future<void> accept_future;
  std::atomic_bool running;
  mutable monitor<std::unordered_map<qp_t, RDMAServerSocket::client_t> > clients;

  dispatch_queue_t queue;
  int fd1;

  void accept(client_t id) const;
  void cm_events() const;

public:
  RDMAServerSocket(const std::string &host, const std::string &port,
                   uint32_t max_wr = 10, int cq_entries = 10);
  ~RDMAServerSocket();
  template <typename Functor>
  std::future<void> operator()(Functor &&functor) const {
    return clients([=](auto &clients) {
      for (const auto &client : clients) {
        functor(client.second.get());
      }
    });
  }

  template <typename Functor>
  auto operator()(const qp_t qp_num, Functor &&functor) const {
    return clients([=](auto &clients) {
      auto client = clients.find(qp_num);
      if (client != std::end(clients)) {
        return functor(client->second.get());
      } else {
        std::ostringstream s;
        s << "rdma_cm_id* for qp " << qp_num << " not found." << std::endl;
        throw std::runtime_error(s.str());
      }
    });
  }

  std::future<void> disconnect(const qp_t qp_num) const;
  void listen(int backlog = 10);
  
  template <typename T>
  std::future<T *> recv_async(const T *local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), local, mr, size);
  }

  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  auto recv_async(const T &local, const ibv_mr *mr,
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
