#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#endif

#include "rdma/RDMAWrapper.hpp"
#include "util/exception.h"

class RDMAClientSocket {
  rdma_id_ptr srq_id;
  rdma_id_ptr id;
  completion_channel cc;
  completion_queue cq;

  uint32_t max_inline_data;

public:
  RDMAClientSocket(const std::string &host, const std::string &port);
  /* TODO: optimize. maybe it is better to make uint32_t/uint16_t from strings,
   * or to have two independent ctors
   */
  //RDMAClientSocket(const uint32_t host, const uint32_t port)
  //    : RDMAClientSocket(std::to_string(port)) {}
  ~RDMAClientSocket();
  void connect() const;
  void disconnect() const;
  
  template <typename T>
  auto send(const T &local, const ibv_mr *mr = nullptr) const {
    using namespace hydra::rdma;
    const void *ptr = address_of(local);
    const size_t size = size_of(local);
    int flags = 0;
    if (size <= max_inline_data) {
      flags |= IBV_SEND_INLINE;
    } else if (mr == nullptr) {
      std::ostringstream ss;
      ss << "Message of " << size << " bytes to large to send inline (max "
         << max_inline_data << ")";
      throw std::runtime_error(ss.str());
    }
    check_zero(rdma_post_send(id.get(), nullptr, ptr, size,
                              const_cast<ibv_mr *>(mr), flags));
  }

  template <typename T>
  void read(const T &local, const ibv_mr *mr, uint64_t remote,
            uint32_t rkey) const {
    using namespace hydra::rdma;
    rdma_read_async__(id.get(), const_cast<void *>(address_of(local)),
                      size_of(local), mr, remote, rkey).get();
  }

  template <typename T, typename U>
  auto read(T *local, ibv_mr *mr, U *remote, uint32_t rkey,
            size_t n_elems = 1) const {
    static_assert(std::is_same<typename std::remove_cv<T>::type,
                               typename std::remove_cv<U>::type>::value,
                  "Need same types.");
    return rdma_read_async__(id.get(), local, n_elems * sizeof(T), mr,
                             reinterpret_cast<uintptr_t>(remote), rkey);
  }

  template <typename T>
  auto recv_async(const T &local, const ibv_mr *mr) const {
    using namespace hydra::rdma;
    const void *ptr = address_of(local);
    const size_t size = size_of(local);
    return rdma_recv_async(srq_id.get(), ptr, mr, size);
  }

  template <typename T>
  auto write(const rdma_ptr<T> &ptr, const uint64_t &remote,
             const uint32_t &rkey, const size_t size = sizeof(T)) const {
    return rdma_write_async(id, ptr, size, remote, rkey);
  }

  template <typename T>
  mr_t register_memory(const ibv_access flags, const T &o) const {
    return ::register_memory(srq_id->pd, flags, o);
  }

  mr_t register_memory(const ibv_access &flags, const void *ptr,
                       const size_t size) const;
};

namespace hydra {
mr_t register_memory(const RDMAClientSocket &socket, const ibv_access &flags,
                     const void *ptr, const size_t size);
}

