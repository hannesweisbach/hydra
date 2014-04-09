#include <rdma/rdma_cma.h>
#include <netdb.h>

#include "Logger.h"
#include "RDMAWrapper.hpp"
#include "RDMAClientSocket.h"
#include "util/concurrent.h"

RDMAClientSocket::RDMAClientSocket(const std::string &host,
                                   const std::string &port)
    : id([=]() {
        ibv_qp_init_attr attr = {};
        attr.cap.max_send_wr = 10;
        attr.cap.max_recv_wr = 10;
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        /* beware, this is a magic mofo */
        attr.cap.max_inline_data = 72;
        attr.sq_sig_all = 1;
        return createCmId(host, port, false, &attr);
      }()),
      running(true), local_heap(*this), remote_heap(*this) {
  ibv_req_notify_cq(id->recv_cq, 0);
  ibv_req_notify_cq(id->send_cq, 0);
// TODO: srq?
#if 1
/* gnarfl. RTFM. */
#ifdef HAVE_LIBDISPATCH
  send_queue = dispatch_queue_create("hydra.cq.send", NULL);
  recv_queue = dispatch_queue_create("hydra.cq.recv", NULL);
  fut_recv =
      rdma_handle_cq_event_async(running, id->recv_cq_channel, recv_queue);
  fut_send =
      rdma_handle_cq_event_async(running, id->send_cq_channel, send_queue);
#else
  fut_recv = rdma_handle_cq_event_async(running, id->recv_cq_channel, 0);
  fut_send = rdma_handle_cq_event_async(running, id->send_cq_channel, 0);
#endif
#else
  rdma_handle_cq_event_async(running, id->recv_cq_channel);
  rdma_handle_cq_event_async(running, id->send_cq_channel);
#endif
}

RDMAClientSocket::RDMAClientSocket(RDMAClientSocket &&other)
    : id(std::move(other.id)), fut_recv(std::move(other.fut_recv)),
      fut_send(std::move(other.fut_send)),
      send_queue(std::move(other.send_queue)),
      recv_queue(std::move(other.send_queue)),
      running(other.running.load()),
      local_heap(std::move(other.local_heap)),
      remote_heap(std::move(other.remote_heap)) {
}

RDMAClientSocket &RDMAClientSocket::operator=(RDMAClientSocket &&other) {
  std::swap(id, other.id);
  std::swap(fut_recv, other.fut_recv);
  std::swap(fut_send, other.fut_send);
  std::swap(send_queue, other.send_queue);
  std::swap(recv_queue, other.send_queue);
  running = other.running.exchange(running);
  std::swap(local_heap, other.local_heap);

  return *this;
}

RDMAClientSocket::~RDMAClientSocket() {
  disconnect();
  running = false;
  fut_recv.get();
  fut_send.get();
  dispatch_release(send_queue);
  dispatch_release(recv_queue);
}

void RDMAClientSocket::connect() const {
  check_zero(rdma_connect(id.get(), nullptr), "rdma_connect");
}

void RDMAClientSocket::disconnect() const { rdma_disconnect(id.get()); }

mr_ptr RDMAClientSocket::register_remote_read(void *ptr, size_t size) const {
  return mr_ptr(check_nonnull(rdma_reg_read(id.get(), ptr, size),
                              "register_read: rdma_reg_read"),
/* this is for debugging purposes only, if sometime proper resource management
 * is required, just change the #if
 */
#if 0
                 rdma_dereg_mr
#else
                [](ibv_mr *) {
    log_err() << "invalid dereg mr";
    assert(false);
  }
#endif
                );
}

mr_ptr RDMAClientSocket::register_local_read(void *ptr, size_t size) const {
  return mr_ptr(check_nonnull(rdma_reg_msgs(id.get(), ptr, size),
                              "register_read: rdma_reg_read"),
/* this is for debugging purposes only, if sometime proper resource management
 * is required, just change the #if
 */
#if 0
                 rdma_dereg_mr
#else
                [](ibv_mr *) {
    log_err() << "invalid dereg mr";
    assert(false);
  }
#endif
                );
}

std::shared_ptr<ibv_mr> allocateMapMemory(const RDMAClientSocket &s,
                                          size_t size, size_t aligment) {
  return std::make_shared<ibv_mr>(); 
         
}
