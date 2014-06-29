#pragma once

#include <memory>
#include <utility>
#include <future>
#include <sstream>
#include <atomic>
#include <utility>
#include <vector>

#include <sys/mman.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "util/concurrent.h"
#include "util/exception.h"
#include "util/Logger.h"

std::ostream &operator<<(std::ostream &ostream, const enum ibv_wc_status& status);
std::ostream &operator<<(std::ostream &ostream, const enum ibv_wc_opcode& opcode);
std::ostream &operator<<(std::ostream &ostream, const enum rdma_cm_event_type& type);
std::ostream &operator<<(std::ostream &ostream, const ibv_mr &mr);
std::ostream &operator<<(std::ostream &ostream, const ibv_device_attr &attr);
std::ostream &operator<<(std::ostream &ostream, const enum ibv_atomic_cap& cap);
std::ostream &operator<<(std::ostream &ostream, const ibv_device &dev);
std::ostream &operator<<(std::ostream &ostream, const ibv_srq_init_attr &attr);
std::ostream &operator<<(std::ostream &ostream, const ibv_srq_attr &attr);

struct mr_deleter {
  void operator()(ibv_mr *mr) { ::ibv_dereg_mr(mr); }
};
using mr_t = std::unique_ptr< ::ibv_mr, mr_deleter>;
using mr_ptr = std::unique_ptr< ::ibv_mr, std::function<void(ibv_mr *)> >;
using rdma_id_ptr = std::unique_ptr< ::rdma_cm_id, decltype(&rdma_destroy_ep)>;
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

class completion_queue;

class completion_channel {
  struct cc_deleter {
    void operator()(ibv_comp_channel *cc) {
      check_zero(::ibv_destroy_comp_channel(cc));
    }
  };
  std::unique_ptr<ibv_comp_channel, cc_deleter> cc;
  std::unique_ptr<std::atomic_bool> run;
  std::thread poller;

  static void loop(ibv_comp_channel *, std::atomic_bool *);

public:
  completion_channel(const rdma_id_ptr &id);
  ~completion_channel();
  void stop();
  friend class completion_queue;
};

class completion_queue {
  class cq {
    struct cq_deleter {
      void operator()(ibv_cq *cq) { check_zero(::ibv_destroy_cq(cq)); }
    };

    mutable std::vector<ibv_wc> wcs;
    const unsigned int outstanding_acks;
    mutable std::atomic_uint events;
    std::unique_ptr<ibv_cq, cq_deleter> cq_;

    void notify() const;
    void ack() const;
    bool poll() const;
    operator ibv_cq *() const;

  public:
    cq(rdma_cm_id *id, ibv_comp_channel *cc, const int entries,
       const size_t completions, const unsigned int outstanding_acks,
       const int completion_vector = 0);
    ~cq();

    bool handle() const;

    friend class completion_queue;
  };
  std::unique_ptr<cq> cq_;

public:
  completion_queue(const rdma_id_ptr &id, const completion_channel &cc,
                   const int entries, const size_t completions = 1,
                   const unsigned int outstanding_acks = 0,
                   const int completion_vector = 0);
  completion_queue(const rdma_id_ptr &id, const int entries,
                   const size_t completions = 1,
                   const unsigned int outstanding_acks = 0,
                   const int completion_vector = 0);
  operator ibv_cq *() const { return *cq_; }
  friend class completion_channel;
};


template <typename T,
          typename = typename std::enable_if<!std::is_pointer<T>::value>::type>
void sendImmediate(::rdma_cm_id *id, const T &o) {
  check_zero(rdma_post_send(id, nullptr,
                            static_cast<void *>(const_cast<T *>(&o)), sizeof(T),
                            nullptr, IBV_SEND_INLINE));
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

  check_zero(functor(f));

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
  check_zero(functor(reinterpret_cast<void*>(f)));

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
  check_zero(functor(reinterpret_cast<void*>(f)));

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
                                    const ibv_mr *mr, uint64_t remote,
                                    uint32_t rkey) {
  auto functor =
      std::bind(rdma_post_read, id, std::placeholders::_1, local, size,
                const_cast<ibv_mr *>(mr), IBV_SEND_SIGNALED, remote, rkey);
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

