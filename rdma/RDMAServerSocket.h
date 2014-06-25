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
  struct client_id_deleter {
    void operator()(rdma_cm_id *id) {
      if (id)
        rdma_destroy_ep(id);
    }
  };
  using client_t = std::unique_ptr<rdma_cm_id, client_id_deleter>;
  ec_ptr ec;
  rdma_id_ptr id;
  completion_channel cc;
  completion_queue cq;
  WorkerThread eventThread;
  std::future<void> accept_future;
  std::atomic_bool running;
  mutable monitor<std::unordered_map<qp_t, RDMAServerSocket::client_t> > clients;


  void accept(client_t id) const;
  void cm_events() const;

  template <typename T>
  auto read_helper(rdma_cm_id *id, const T &local, const size_t size,
                   const ibv_mr *mr, const uint64_t &remote,
                   const uint32_t &rkey, std::true_type) {
    return rdma_read_async__(id, local, size, mr, remote, rkey);
  }

  template <typename T>
  auto read_helper(rdma_cm_id *id, T &local, const size_t size,
                   const ibv_mr *mr, const uint64_t &remote,
                   const uint32_t &rkey, std::false_type) {
    return rdma_read_async__(id, &local, size, mr, remote, rkey);
  }

  template <typename T>
  auto recv_async_helper(rdma_cm_id *id, const T &local, const ibv_mr *mr,
                         size_t size, std::true_type) {
    return rdma_recv_async(id, local, mr, size);
  }

  template <typename T>
  auto recv_async_helper(rdma_cm_id *id, const T &local, const ibv_mr *mr,
                         size_t, std::false_type) {
    return rdma_recv_async(id, &local, mr, sizeof(T));
  }

public:
  RDMAServerSocket(const std::string &host, const std::string &port,
                   uint32_t max_wr = 10, int cq_entries = 10);
  ~RDMAServerSocket();
  template <typename Functor> void operator()(Functor &&functor) const {
    return clients([=](const auto &clients) {
      for (const auto &client : clients) {
        functor(client.second.get());
      }
    });
  }

  template <typename Functor>
  auto operator()(const qp_t qp_num, Functor &&functor)
      const -> typename std::result_of<Functor(rdma_cm_id *)>::type {
    return clients([=](const auto &clients) {
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

  void disconnect(const qp_t qp_num) const;
  void listen(int backlog = 10);
  rdma_cm_id * find(const qp_t qp_num) const;
  
  template <typename T>
  std::future<T *> recv_async(const T *local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), local, mr, size);
  }

  template <typename T>
  auto recv_async(const T &local, const ibv_mr *mr, size_t size = sizeof(T)) {
    return recv_async_helper(id.get(), local, mr, size, std::is_pointer<T>());
  }

  template <typename T>
  auto recv_async(const qp_t qp_num, const T &local, const ibv_mr *mr,
                  size_t size = sizeof(T)) {
    return recv_async_helper(find(qp_num), local, mr, size,
                             std::is_pointer<T>());
  }

  template <typename T>
  auto read(const qp_t &qp_num, T &local, const ibv_mr *mr,
            const uint64_t &remote, const uint32_t &rkey,
            const size_t size = sizeof(T)) {
      return read_helper(find(qp_num), local, size, mr, remote, rkey,
                         std::is_pointer<T>());
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
