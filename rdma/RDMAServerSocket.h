#pragma once

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <unordered_map>
#include <iterator>
#include <algorithm>

#include "RDMAWrapper.hpp"
#include "util/WorkerThread.h"
#include "util/concurrent.h"

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#endif

class RDMAServerSocket;

namespace hydra {
mr_t register_memory(const RDMAServerSocket &socket, const ibv_access &flags,
                     const void *ptr, const size_t size);
}

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
  std::vector<rdma_id_ptr> ids;
  completion_channel cc;
  completion_queue cq;
  WorkerThread eventThread;
  std::atomic_bool running;
  mutable monitor<std::vector<RDMAServerSocket::client_t> > clients;

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
  RDMAServerSocket(std::vector<std::string> hosts, const std::string &port,
                   uint32_t max_wr = 16383, int cq_entries = 131071);
  RDMAServerSocket(const std::string &host, const std::string &port,
                   uint32_t max_wr = 16383, int cq_entries = 131071);
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
    return clients([ functor = std::move(functor), qp_num ]
        (const auto & clients) {
          auto client = std::lower_bound(std::begin(clients),
              std::end(clients), qp_num,
              [](const auto &client, const qp_t &qp_num) {
                return client->qp->qp_num < qp_num;
          });
          if ((*client)->qp->qp_num == qp_num) {
            return functor(client->get());
          } else {
            std::ostringstream s;
            s << "rdma_cm_id* for qp " << qp_num << " not found." << std::endl;
            throw std::runtime_error(s.str());
          }
    });
  }

  void disconnect(const qp_t qp_num) const;
  void listen(int backlog = 10);

  template <typename T>
  std::future<T *> recv_async(const T *local, const ibv_mr *mr,
                              size_t size = sizeof(T)) {
    return rdma_recv_async(id.get(), local, mr, size);
  }

  template <typename T>
  auto recv_async(const T &local, const ibv_mr *mr) {
    using namespace hydra::rdma;
    const void * ptr = address_of(local);
    const size_t size = size_of(local);
    return rdma_recv_async(id.get(), ptr, mr, size);
  }

  template <typename T>
  auto recv_async(const qp_t qp_num, const T &local, const ibv_mr *mr,
                  size_t size = sizeof(T)) {
    (*this)(qp_num, [&local, qp_num, mr, size, this](rdma_cm_id *id) {
      return recv_async_helper(id, local, mr, size, std::is_pointer<T>());
    });
  }

  template <typename T>
  auto read(const qp_t &qp_num, T &local, const ibv_mr *mr,
            const uint64_t &remote, const uint32_t &rkey,
            const size_t size = sizeof(T)) {
    (*this)(qp_num, [&local, remote, rkey, mr, size, this](rdma_cm_id *id) {
      return read_helper(id, local, size, mr, remote, rkey,
                         std::is_pointer<T>());
    });
  }

  template <typename T>
  mr_t register_memory(const ibv_access flags, const T &o) const {
    return ::register_memory(id->pd, flags, o);
  }

  mr_t register_memory(const ibv_access &flags, const void *ptr,
                       const size_t size) const;
};

