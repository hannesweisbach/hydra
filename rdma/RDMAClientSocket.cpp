#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <unistd.h>

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
      local_heap(*this), remote_heap(*this) {
  ibv_req_notify_cq(id->recv_cq, 0);
  ibv_req_notify_cq(id->send_cq, 0);
// TODO: srq?
  send_queue = dispatch_queue_create("hydra.cq.send", NULL);
  recv_queue = dispatch_queue_create("hydra.cq.recv", NULL);
  int fds[2];
  pipe(fds);
  fd1 = fds[1];
  fut_recv =
      rdma_handle_cq_event_async(id->recv_cq_channel, recv_queue, fds[0]);
  pipe(fds);
  fd2 = fds[1];
  fut_send =
      rdma_handle_cq_event_async(id->send_cq_channel, send_queue, fds[0]);
}

RDMAClientSocket::~RDMAClientSocket() {
  disconnect();
  char c;
  write(fd1, &c, 1);
  write(fd2, &c, 1);
  fut_recv.get();
  fut_send.get();
  dispatch_release(send_queue);
  dispatch_release(recv_queue);
  close(fd1);
  close(fd2);
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

