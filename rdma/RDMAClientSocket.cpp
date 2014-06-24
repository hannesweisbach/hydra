#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <unistd.h>

#include "Logger.h"
#include "RDMAWrapper.hpp"
#include "RDMAClientSocket.h"
#include "util/concurrent.h"

RDMAClientSocket::RDMAClientSocket(const std::string &host,
                                   const std::string &port)
    : srq_id(createCmId(host, port, false, nullptr)),
      id(nullptr, [](rdma_cm_id *) {}), cc(srq_id), cq(srq_id, cc, 4, 1, 0),
      local_heap(*this), remote_heap(*this) {

  ibv_srq_init_attr srq_attr = { nullptr, { 4, 1, 0 } };
  check_zero(rdma_create_srq(srq_id.get(), nullptr, &srq_attr));

  ibv_qp_init_attr attr = {};
  attr.cap.max_send_wr = 4;
  attr.cap.max_recv_wr = 4;
  attr.cap.max_send_sge = 1;
  attr.cap.max_recv_sge = 1;
  attr.recv_cq = cq;
  attr.send_cq = cq;
  attr.srq = srq_id->srq;
  attr.cap.max_inline_data = 72;
  attr.sq_sig_all = 1;
  id = createCmId(host, port, false, &attr);
}

RDMAClientSocket::~RDMAClientSocket() {
  disconnect();
  cc.stop();
  id.reset();
}

void RDMAClientSocket::connect() const {
  check_zero(rdma_connect(id.get(), nullptr));
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

