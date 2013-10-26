#include <netdb.h>  // getaddrinfo
#include <assert.h>
#include <stdexcept>
#include <iostream>
#include <cstring>
#include <rdma/rdma_cma.h>

#include "RDMAServer.hpp"

#if 0
class RDMAConnection {
  public:
    RDMAConnection() {
      struct ibv_qp_init_attr qp_attr = {};
      qp_attr->send_cq = ctx->cq;
      qp_attr->recv_cq = ctx->cq;
      qp_attr->qp_type = IBV_QPT_RC; /* reliable connection-oriented */

      /* Negotiate minimum capabilities. */
      qp_attr->cap.max_send_wr = 10;
      qp_attr->cap.max_recv_wr = 10;
      qp_attr->cap.max_send_sge = 1; /* 1 scatter-gather element per send/recv */
      qp_attr->cap.max_recv_sge = 1;
    
      if(rdma_create_qp(event->id, ctx->pd, &qp_attr)) {
        perror("rdma_create_qp");
        return true;
      }
    }

};
#endif

class RDMAException : public std::runtime_error {
  public:
    explicit RDMAException(const char * what, const char * err) : runtime_error(std::string(what) + std::string(" failed: ") + std::string(err)) {}
    explicit RDMAException(const std::string& what) : runtime_error(what) {}
};

RDMAServer::RDMAServer() :
  ec(nullptr),
  id(nullptr) {}

RDMAServer::RDMAServer(const std::string& port) : RDMAServer(std::string(), port) {}

RDMAServer::RDMAServer(const std::string& host, const std::string& port) : RDMAServer() {
  if((ec = rdma_create_event_channel()) == nullptr)
    throw new RDMAException("rdma_create_event_channel", strerror(errno));
  
  if(rdma_create_id(ec, &id, NULL, RDMA_PS_TCP))
    throw new RDMAException("rdma_create_id", strerror(errno));
  
  /* either specified ip addr or nullptr (all interfaces) */
  const char * hostname = (host.empty()) ? nullptr : host.c_str();

  struct addrinfo *ais;
  struct addrinfo hints = {};
  hints.ai_flags = AI_PASSIVE | AI_ADDRCONFIG | AI_NUMERICSERV;

  int err = getaddrinfo(hostname, port.c_str(), &hints, &ais);
  if(err)
    throw new RDMAException("getaddrinfo", gai_strerror(err));

  for(struct addrinfo * ai = ais; ai != nullptr; ai = ai->ai_next) {
    if(rdma_bind_addr(id, ai->ai_addr))
      throw new RDMAException("rdma_bind_addr", strerror(errno));
  }

  freeaddrinfo(ais);

  if(rdma_listen(id, 10))
    throw new RDMAException("rdma_listen", strerror(errno));

  //TODO: logging facility
  std::cout << "Started server on port " << port;
}

RDMAServer::~RDMAServer() {
  if(id != nullptr)
    rdma_destroy_id(id);
  if(ec != nullptr)
    rdma_destroy_event_channel(ec);
}
