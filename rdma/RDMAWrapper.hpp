#pragma once

#include <memory>
#include <utility>
#include <future>
#include <sstream>
#include <atomic>

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
    -> std::future<T> {
  auto promise = std::make_shared<std::promise<T> >();
  std::function<void(ibv_wc &)> *f = new std::function<void(ibv_wc &)>();
  *f = [=](ibv_wc &wc) {
    log_info() << (enum ibv_wc_status)wc.status << " "
               << ": " << wc.byte_len << std::endl;
    try {
      if (wc.status == IBV_WC_SUCCESS) {
        promise->set_value(value);
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
std::future<T *> rdma_recv_async(rdma_cm_id *id, const T *local,
                                 const ibv_mr *mr, size_t size = sizeof(T)) {
  auto func =
      std::bind(rdma_post_recv, id, std::placeholders::_1,
                const_cast<void *>(static_cast<const void *>(local)), size,
                const_cast<ibv_mr *>(mr));
  return async_rdma_operation(
      func, const_cast<typename std::remove_const<T>::type*>(local));
}

template <typename T>
std::future<T *> rdma_read_async__(rdma_cm_id *id, T *local, size_t size,
                                   ibv_mr *mr, uint64_t remote, uint32_t rkey) {
  auto functor = std::bind(rdma_post_read, id, std::placeholders::_1, local,
                           size, mr, IBV_SEND_SIGNALED, remote, rkey);
  return async_rdma_operation(functor, local);
}

/*keep*/
template <typename T>
[[deprecated]] std::future<T *> rdma_read_async(std::shared_ptr<rdma_cm_id>& id, T *local,
                                 uint64_t remote, uint32_t rkey,
                                 size_t size = sizeof(T)) {
  return rdma_read_async__(id.get(), local, remote, rkey, size);
}

template <typename T>
[[deprecated]] std::shared_ptr< ::ibv_mr> registerMemory(const rdma_id_ptr &id, T *item,
                                          size_t size = sizeof(T)) {
  return std::shared_ptr< ::ibv_mr>(
      check_nonnull(::rdma_reg_msgs(id.get(), static_cast<void *>(item), size)),
      ::rdma_dereg_mr);
}

void dereg_debug(ibv_mr *mr);

class RDMAReadMem {
  std::shared_ptr<void> ptr;
  std::shared_ptr<ibv_mr /*, decltype(& ::rdma_dereg_mr)*/> mr_;

public:
  RDMAReadMem(rdma_id_ptr &id, size_t size, size_t alignment = 4096) {
    (void)(id);
    (void)(size);
    (void)(alignment);
  }
  RDMAReadMem(std::shared_ptr< ::rdma_cm_id> &id, size_t size,
              size_t alignment = 4096)
      : ptr([=]() {
          (void)(alignment);
#if 0
              void *p = check_nonnull(malloc(alignment + size));
              uintptr_t p_ = (uintptr_t)p + (alignment - ((uintptr_t)p & (alignment - 1)));
              log_info() << p << " " << size << " " << alignment << " " << (void*)p_;
              p = (void*)p_;
#elif 0
              void *p = nullptr;
              int err = posix_memalign(&p, alignment, size);
              check_zero(err);
              check_nonnull(p);
#else
              void *p =
                  check_nonnull(mmap(nullptr, size, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_ANONYMOUS, -1, 0));
#endif
              memset(p, 'k', size);
              return p;
            }(),
#if 0
            ::free
#else
            [=](void *p_) { ::munmap(p_, size); }
#endif
            ),
#if 1
        mr_(check_nonnull(rdma_reg_read(id.get(), ptr.get(), size)),
            /*::rdma_dereg_mr*/ dereg_debug)
#else
        mr_(check_nonnull(
                ibv_reg_mr(id->pd, ptr.get(), size, IBV_ACCESS_REMOTE_READ)),
            ::ibv_dereg_mr)
#endif
  {
    memset(ptr.get(), 'b', size);
  }
  void *get() const { return ptr.get(); }
  uint32_t rkey() const { return mr_->rkey; }
  size_t size() const { return mr_->length; }
  std::shared_ptr<void> copy_ptr() const { return ptr; }
  std::shared_ptr<ibv_mr> mr() { return mr_; }
  //  const ibv_mr * mr() { return mr_.get(); }
};

class RDMABuf {
  std::shared_ptr<void> ptr;
  std::shared_ptr<ibv_mr /*, decltype(& ::rdma_dereg_mr)*/> mr_;

public:
  RDMABuf(std::shared_ptr< ::rdma_cm_id> &id, size_t size,
          size_t alignment = 4096)
      : ptr([=]() {
              void *p = nullptr;
              int err = posix_memalign(&p, alignment, size);
              check_zero(err);
              check_nonnull(p);
              return p;
            }(),
            ::free),
        mr_(check_nonnull(rdma_reg_msgs(id.get(), ptr.get(), size)),
            ::rdma_dereg_mr) {}
  void *get() const { return ptr.get(); }
  uint32_t rkey() const { return mr_->rkey; }
  size_t size() const { return mr_->length; }
  std::shared_ptr<void> copy_ptr() const { return ptr; }
  std::shared_ptr<ibv_mr> mr() { return mr_; }
};

