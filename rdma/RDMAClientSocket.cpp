#include <rdma/rdma_cma.h>

#include "Logger.h"
#include "RDMAWrapper.hpp"
#include "RDMAClientSocket.h"
#include "util/concurrent.h"

namespace hydra {
mr_t register_memory(const RDMAClientSocket &socket, const ibv_access &flags,
                     const void *ptr, const size_t size) {
  return socket.register_memory(flags, ptr, size);
}
}

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
  attr.sq_sig_all = 1;

  for (max_inline_data = 1;; max_inline_data = attr.cap.max_inline_data + 1) {
    attr.cap.max_inline_data = max_inline_data;

    try {
      id = createCmId(host, port, false, &attr);
    }
    catch (...) {
      --max_inline_data;
      attr.cap.max_inline_data = max_inline_data;
      id = createCmId(host, port, false, &attr);
      break;
    }
  }
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

mr_t RDMAClientSocket::register_memory(const ibv_access &flags, const void *ptr,
                                       const size_t size) const {
  return ::register_memory(srq_id->pd, flags, ptr, size);
}

