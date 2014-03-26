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

std::shared_ptr< ::rdma_event_channel> createEventChannel();

typedef std::unique_ptr< ::ibv_mr, std::function<void(ibv_mr *)> > mr_ptr;
typedef std::unique_ptr< ::rdma_cm_id, decltype(&rdma_destroy_ep)> rdma_id_ptr;
typedef std::unique_ptr< ::ibv_comp_channel,
                         decltype(& ::ibv_destroy_comp_channel)>
comp_channel_ptr;
typedef std::unique_ptr< ::ibv_cq, decltype(&ibv_destroy_cq)> cq_ptr;
using qp_t = decltype(ibv_wc::qp_num);

template <typename T>
using pointer_t = std::unique_ptr<T, std::function<void(T *)> >;
template <typename T> using rdma_ptr = std::pair<pointer_t<T>, ibv_mr *>;

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
auto async_rdma_operation(RDMAFunctor &&functor, T value, Args &&... args)
    -> std::future<qp_t> {
  auto promise = std::make_shared<std::promise<qp_t> >();
  std::function<void(ibv_wc &)> *f = new std::function<void(ibv_wc &)>();
  *f = [=](ibv_wc &wc) {
    log_info() << (enum ibv_wc_status)wc.status << " "
               << ": " << wc.byte_len << std::endl;
    try {
      if (wc.status == IBV_WC_SUCCESS) {
        //promise->set_value(value);
        promise->set_value(wc.qp_num);
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
  check_zero(functor(f, std::forward<Args>(args)...), __func__);

  return promise->get_future();
}

template <typename T>
[[deprecated]] std::future<std::shared_ptr<T> > rdma_recv_async(rdma_cm_id *id,
                                                 std::shared_ptr<T> local,
                                                 std::shared_ptr<ibv_mr> &mr) {
  auto func = std::bind(rdma_post_recv, id, std::placeholders::_1, local.get(),
                        sizeof(T), mr.get());
  return async_rdma_operation(func, local);
}

template <typename T>
std::future<qp_t> rdma_recv_async(rdma_cm_id *id, const T *local,
                                 const ibv_mr *mr, size_t size = sizeof(T)) {
  auto func =
      std::bind(rdma_post_recv, id, std::placeholders::_1,
                const_cast<void *>(static_cast<const void *>(local)), size,
                const_cast<ibv_mr *>(mr));
  return async_rdma_operation(
      func, const_cast<typename std::remove_const<T>::type*>(local));
}

template <typename T>
std::future<qp_t> rdma_read_async__(rdma_cm_id *id, T *local, size_t size,
                                   ibv_mr *mr, uint64_t remote, uint32_t rkey) {
  auto functor = std::bind(rdma_post_read, id, std::placeholders::_1, local,
                           size, mr, IBV_SEND_SIGNALED, remote, rkey);
  return async_rdma_operation(functor, local);
}

/*keep*/
template <typename T>
[[deprecated]] std::future<T *> rdma_read_async(std::shared_ptr<rdma_cm_id> &id,
                                                T *local, uint64_t remote,
                                                uint32_t rkey,
                                                size_t size = sizeof(T)) {
  return rdma_read_async__(id.get(), local, remote, rkey, size);
}

