#include <stdexcept>
#include <future>

#include <sys/select.h>
#include <assert.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <unistd.h>

#include "RDMAServerSocket.h"
#include "RDMAWrapper.hpp"
#include "util/Logger.h"
#include "util/concurrent.h"
#include "util/epoll.h"

namespace hydra {
mr_t register_memory(const RDMAServerSocket &socket, const ibv_access &flags,
                     const void *ptr, size_t size) {
  return socket.register_memory(flags, ptr, size);
}
}

RDMAServerSocket::RDMAServerSocket(const std::string &host,
                                   const std::string &port, uint32_t max_wr,
                                   int cq_entries)
    : RDMAServerSocket(std::vector<std::string>({ host }), port, max_wr,
                       cq_entries) {}

RDMAServerSocket::RDMAServerSocket(std::vector<std::string> hosts,
                                   const std::string &port, uint32_t max_wr,
                                   int cq_entries)
    : ec(createEventChannel()), id(createCmId(hosts.back(), port, true)),
      cc(id), cq(id, cc, cq_entries, 1, 0), running(true) {
  assert(max_wr);

  check_zero(rdma_migrate_id(id.get(), ec.get()));

  ibv_srq_init_attr srq_attr = { nullptr, { max_wr, 1, 0 } };
  check_zero(rdma_create_srq(id.get(), nullptr, &srq_attr));

  log_info() << "Created id " << id.get() << " " << (void *)this;
  hosts.pop_back();

  for (const auto &host : hosts) {
    ibv_qp_init_attr attr = {};
    attr.cap.max_send_wr = 256;
    attr.cap.max_recv_wr = 0;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 0;
    attr.recv_cq = cq;
    attr.send_cq = cq;
    attr.srq = id->srq;
    attr.cap.max_inline_data = 72;
    attr.sq_sig_all = 1;
    auto client_id = createCmId(host, port, true, &attr, id->pd);

    check_zero(rdma_migrate_id(client_id.get(), ec.get()));

    ids.push_back(std::move(client_id));

    log_info() << srq_attr;
  }

  cm_events();
  if(id->verbs) {
    ibv_device_attr attr;
    check_zero(ibv_query_device(id->verbs, &attr));
    log_info() << attr;
  }
}

RDMAServerSocket::~RDMAServerSocket() {
  running = false;
  for (auto &&id : ids) {
    rdma_disconnect(id.get());
  }
  rdma_disconnect(id.get());
  // should cause an event to be fired, but doesn't.
  // use epoll w/ timeouts instead
  id.reset();
}

void RDMAServerSocket::disconnect(const qp_t qp_num) const {
  (*this)(qp_num, [qp_num](rdma_cm_id *client) { rdma_disconnect(client); });
}

void RDMAServerSocket::listen(int backlog) {
  if (rdma_listen(id.get(), backlog))
    throw_errno("rdma_listen");
  
  log_info() << "server listening on "
             << inet_ntoa(id->route.addr.src_sin.sin_addr) << ":"
             << ntohs(id->route.addr.src_sin.sin_port) << " " << id.get() << " " << (void*) this;
}

void RDMAServerSocket::accept(client_t client_id) const {
  ibv_qp_init_attr qp_attr = {};
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = 256;
  qp_attr.cap.max_recv_wr = 0;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 0;
  qp_attr.cap.max_inline_data = 72;
  qp_attr.recv_cq = cq;
  qp_attr.send_cq = cq;
  qp_attr.srq = id->srq;
  qp_attr.sq_sig_all = 1;

  check_zero(rdma_create_qp(client_id.get(), NULL, &qp_attr));

  check_zero(rdma_accept(client_id.get(), nullptr));

  clients([client_id = std::move(client_id)](auto && clients) mutable {
    auto pos = std::lower_bound(std::begin(clients), std::end(clients),
                                client_id->qp->qp_num,
                                [](const auto &client, const qp_t &qp_num) {
      return client->qp->qp_num < qp_num;
    });
    clients.insert(pos, std::move(client_id));
  });
}

void RDMAServerSocket::cm_events() const {
  eventThread.send([=]() {
    hydra::util::epoll poll;
    epoll_event poll_event;
    poll_event.events = EPOLLIN;
    poll_event.data.fd = ec->fd;

    poll.add(ec->fd, &poll_event);

    while (running) {
      rdma_cm_event *cm_event = nullptr;

      int ret = poll.wait(&poll_event, 1, 2000);
      if (ret) {
        if (poll_event.data.fd == ec->fd) {
          check_zero(rdma_get_cm_event(ec.get(), &cm_event));
          check_zero(cm_event->status);
          if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            accept(client_t(cm_event->id));
          } else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
            rdma_disconnect(cm_event->id);
          }
          check_zero(rdma_ack_cm_event(cm_event));
        } else {
          log_err() << "Unkown fd " << poll_event.data.fd << " set. Expected "
                    << id->channel->fd;
          std::terminate();
        }
      }
    }
  });
}

mr_t RDMAServerSocket::register_memory(const ibv_access &flags, const void *ptr,
                                       const size_t size) const {
  return ::register_memory(id->pd, flags, ptr, size);
}

