#pragma once

#include <memory>
#include <utility>
#include <future>
#include <sstream>
#include <atomic>
#include <utility>

#include <sys/mman.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "util/concurrent.h"
#include "util/exception.h"
#include "util/Logger.h"

#ifdef HAVE_LIBDISPATCH
std::future<void> rdma_handle_cq_event_async(
    std::atomic_bool &running, ibv_comp_channel *cc,
    async_queue_type queue =
        dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));

#else
std::future<void> rdma_handle_cq_event_async(std::atomic_bool &running,
                                             ibv_comp_channel *cc,
                                             async_queue_type queue = 0);
#endif

std::ostream &operator<<(std::ostream &ostream, const enum ibv_wc_status& status);
std::ostream &operator<<(std::ostream &ostream, const enum ibv_wc_opcode& opcode);
std::ostream &operator<<(std::ostream &ostream, const enum rdma_cm_event_type& type);
std::ostream &operator<<(std::ostream &ostream, const ibv_mr &mr);
std::ostream &operator<<(std::ostream &ostream, const ibv_device_attr &attr);
std::ostream &operator<<(std::ostream &ostream, const enum ibv_atomic_cap& cap);
std::ostream &operator<<(std::ostream &ostream, const ibv_device &dev);

using mr_ptr = std::unique_ptr< ::ibv_mr, std::function<void(ibv_mr *)> >;
using rdma_id_ptr = std::unique_ptr< ::rdma_cm_id, decltype(&rdma_destroy_ep)>;
using comp_channel_ptr = std::unique_ptr<
    ::ibv_comp_channel, decltype(& ::ibv_destroy_comp_channel)>;
using cq_ptr = std::unique_ptr< ::ibv_cq, decltype(&ibv_destroy_cq)>;
using qp_t = decltype(ibv_wc::qp_num);
static_assert(std::is_same<qp_t, decltype(ibv_qp::qp_num)>::value,
              "QP number type must be the same in ibv_wc and ibv_qp");

template <typename T>
using pointer_t = std::unique_ptr<T, std::function<void(T *)> >;
template <typename T> using rdma_ptr = std::pair<pointer_t<T>, ibv_mr *>;

using ec_ptr = std::unique_ptr< ::rdma_event_channel,
                                decltype(&rdma_destroy_event_channel)>;

ec_ptr createEventChannel();

rdma_id_ptr createCmId(const std::string &host, const std::string &port,
                       const bool passive = false,
                       ibv_qp_init_attr *attr = nullptr);
comp_channel_ptr createCompChannel(rdma_id_ptr &id);
cq_ptr createCQ(rdma_id_ptr &id, int entries, void *context,
                comp_channel_ptr &channel, int completion_vector);

std::shared_ptr< ::ibv_pd>
createProtectionDomain(std::shared_ptr< ::rdma_cm_id> id);

template <typename T,
          typename = typename std::enable_if<!std::is_pointer<T>::value>::type>
void sendImmediate(::rdma_cm_id *id, const T &o) {
  check_zero(rdma_post_send(id, nullptr,
                            static_cast<void *>(const_cast<T *>(&o)), sizeof(T),
                            nullptr, IBV_SEND_INLINE),
             __func__);
}

template <typename RDMAFunctor, typename Continuation>
auto rdma_continuation(RDMAFunctor &&functor, Continuation &&continuation)
    -> std::future<std::result_of<Continuation> > {
  auto promise =
      std::make_shared<std::promise<std::result_of<Continuation> > >();
  std::function<void(ibv_wc &)> *f = new std::function<void(ibv_wc &)>();
  *f = [=](ibv_wc &wc) {
    log_info() << (enum ibv_wc_status)wc.status << " "
               << ": " << wc.byte_len << std::endl;
    // TODO set exception when wc.status != IBV_WC_SUCCESS
    try {
      promise->set_value(continuation());
    }
    catch (std::exception &) {
      promise->set_exception(std::current_exception());
    }
    delete f;
  };

  check_zero(functor(f), __func__);

  return promise->get_future();
}

template <typename T, typename RDMAFunctor, typename... Args>
auto async_rdma_operation2(RDMAFunctor &&functor, T value)
    -> std::future<T> {
  auto promise = std::make_shared<std::promise<T> >();
  std::function<void(ibv_wc &)> *f = new std::function<void(ibv_wc &)>();
  *f = [=, value = std::move(value)](ibv_wc &wc) {
    log_info() << (enum ibv_wc_status)wc.status << " "
               << ": " << wc.byte_len << std::endl;
    try {
      if (wc.status == IBV_WC_SUCCESS) {
        promise->set_value(std::move(value));
      } else {
        std::ostringstream s;
        s << wc.opcode << " resulted in " << wc.status;
        throw std::runtime_error(s.str());
      }
    }
    catch (std::exception &) {
      promise->set_exception(std::current_exception());
    }
    delete f;
  };

  log_info() << "wr_id: " << (void *)f;
  check_zero(functor(reinterpret_cast<void*>(f)), __func__);

  return promise->get_future();
}

template <typename RDMAFunctor>
auto async_rdma_operation(RDMAFunctor &&functor)
    -> std::future<qp_t> {
  auto promise = std::make_shared<std::promise<qp_t> >();
  std::function<void(const ibv_wc &)> *f =
      new std::function<void(const ibv_wc &)>();
  *f = [=](const ibv_wc &wc) {
    try {
      if (wc.status == IBV_WC_SUCCESS) {
        promise->set_value(wc.qp_num);
      } else {
        log_info() << (enum ibv_wc_status)wc.status << " : " << wc.byte_len
                   << " wr_id: " << reinterpret_cast<void *>(f) << " ("
                   << reinterpret_cast<void *>(wc.wr_id) << ") "
                   << " promise: " << reinterpret_cast<void *>(promise.get());
        std::ostringstream s;
        s << wc.opcode << " resulted in " << wc.status;
        throw std::runtime_error(s.str());
      }
    }
    catch (std::exception &) {
      promise->set_exception(std::current_exception());
    }
    delete f;
  };

  //log_info() << "wr_id: " << reinterpret_cast<void *>(f)
  //           << " promise: " << reinterpret_cast<void *>(promise.get());
  check_zero(functor(reinterpret_cast<void*>(f)), __func__);

  return promise->get_future();
}

template <typename T>
std::future<qp_t> rdma_recv_async(rdma_cm_id *id, const T *local,
                                 const ibv_mr *mr, size_t size = sizeof(T)) {
  auto func =
      std::bind(rdma_post_recv, id, std::placeholders::_1,
                const_cast<void *>(static_cast<const void *>(local)), size,
                const_cast<ibv_mr *>(mr));
  return async_rdma_operation(func);
}

template <typename T>
auto rdma_recv_async(const rdma_id_ptr &id, const rdma_ptr<T> &ptr,
                     const size_t size = sizeof(T)) {
  auto func = std::bind(rdma_post_recv, id.get(), std::placeholders::_1,
                        static_cast<void *>(ptr.first.get()), size, ptr.second);
  return async_rdma_operation(func);
}

template <typename T>
auto rdma_read_async(const rdma_id_ptr &id, const T &buffer, const size_t size,
                     const uint64_t remote, const uint32_t rkey) {
  auto functor = std::bind(rdma_post_read, id.get(), std::placeholders::_1,
                           buffer.first.get(), size, buffer.second,
                           IBV_SEND_SIGNALED, remote, rkey);
  return async_rdma_operation(functor);
}

template <typename T>
std::future<qp_t> rdma_read_async__(rdma_cm_id *id, T *local, size_t size,
                                    ibv_mr *mr, uint64_t remote,
                                    uint32_t rkey) {
  auto functor = std::bind(rdma_post_read, id, std::placeholders::_1, local,
                           size, mr, IBV_SEND_SIGNALED, remote, rkey);
  return async_rdma_operation(functor);
}

/*keep*/
template <typename T>
[[deprecated]] std::future<T *> rdma_read_async(std::shared_ptr<rdma_cm_id> &id,
                                                T *local, uint64_t remote,
                                                uint32_t rkey,
                                                size_t size = sizeof(T)) {
  return rdma_read_async__(id.get(), local, remote, rkey, size);
}

