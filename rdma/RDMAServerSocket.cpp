#include <stdexcept>
#include <future>

#include <sys/select.h>
#include <assert.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>

#include "RDMAServerSocket.h"
#include "RDMAWrapper.hpp"
#include "util/Logger.h"
#include "util/concurrent.h"

RDMAServerSocket::RDMAServerSocket(const std::string &host,
                                   const std::string &port, uint32_t max_wr,
                                   int cq_entries)
    : id(createCmId(host, port, true)), cc(createCompChannel(id)),
      cq(createCQ(id, cq_entries, nullptr, cc, 0)), running(true) {
  assert(max_wr);
        log_info() << "Created id " << id.get() << " " << (void*) this;
#if 1
  /* TODO: throw, if id is not valid here */
  /* on second thought, let one of the calls below fail, if id is not valid */
  ibv_srq_init_attr srq_attr = { nullptr, { max_wr, 1, 0 } };
  // srq_attr.attr.max_wr = max_wr;
  // srq_attr.attr.max_sge = 1;
  if (rdma_create_srq(id.get(), nullptr, &srq_attr))
    throw_errno("rdma_create_srq");
#endif

#if 1
  if (ibv_req_notify_cq(cq.get(), 0))
    throw_errno("ibv_req_notify");
#endif
  async_fut = rdma_handle_cq_event_async(running, cc.get());
}

RDMAServerSocket::~RDMAServerSocket() {
  running = false;
  rdma_destroy_srq(id.get());
  assert(false);
  //rdma_disconnect(id.get());
}

void RDMAServerSocket::listen(int backlog) {
  if (rdma_listen(id.get(), backlog))
    throw_errno("rdma_listen");
  
  log_info() << "server listening on "
             << inet_ntoa(id->route.addr.src_sin.sin_addr) << ":"
             << ntohs(id->route.addr.src_sin.sin_port) << " " << id.get() << " " << (void*) this;
}

std::future<RDMAServerSocket::client_t> RDMAServerSocket::accept() {
  /* maybe do rdma_get_cm_event */
  //return std::async(std::launch::async, [=] {
  return acceptThread.send([=] {
#ifndef HAVE_LIBDISPATCH
  
#else
  //return hydra::async([=] {
#endif
    rdma_cm_id *clientId;
    if (rdma_get_request(id.get(), &clientId))
      throw_errno("rdma_get_request");

    log_trace() << "Connection request";

    ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 10;
    qp_attr.cap.max_recv_wr = 10;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 72;
    qp_attr.recv_cq = cq.get();
    qp_attr.send_cq = cq.get();
    qp_attr.srq = id->srq;
    qp_attr.sq_sig_all = 0;
    if (rdma_create_qp(clientId, NULL, &qp_attr))
      throw_errno("rdma_create_qp");

    log_info() << "Max inline data: " << qp_attr.cap.max_inline_data;
    /* WTF? why did I set the srq in qp_attr then?
     * This shit is seriously broken.
     */
    /* Set the new connection to use our SRQ */
    clientId->srq = id->srq;

    if (rdma_accept(clientId, nullptr))
      throw_errno("rdma_accept");

    log_trace() << "Accepted Connection request";

    return client_t(clientId, [](rdma_cm_id *id) {
        assert(false);
      rdma_destroy_qp(id);
      log_info() << "rdma_destroy_qp(" << (void*)id << ")";
      rdma_destroy_id(id);
      log_info() << "rdma_destroy_id(" << (void*)id << ")";
    });
  });
}

void RDMAServerSocket::sendImmediate(rdma_cm_id * id, const void * buffer, const size_t size) const {
  if(rdma_post_send(id, nullptr, buffer, size, nullptr, IBV_SEND_INLINE | IBV_SEND_FENCE))
    log_error() << "ibv_post_send " << strerror(errno);
}

mr_ptr RDMAServerSocket::register_remote_read(void *ptr, size_t size) const {
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

mr_ptr RDMAServerSocket::register_local_read(void *ptr, size_t size) const {
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


