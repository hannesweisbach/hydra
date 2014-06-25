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

RDMAServerSocket::RDMAServerSocket(const std::string &host,
                                   const std::string &port, uint32_t max_wr,
                                   int cq_entries)
    : RDMAServerSocket(std::vector<std::string>({ host }), port, max_wr,
                       cq_entries) {}

RDMAServerSocket::RDMAServerSocket(std::vector<std::string> hosts,
                                   const std::string &port, uint32_t max_wr,
                                   int cq_entries)
    : ec(createEventChannel()), id(createCmId(hosts.back(), port, true)),
      cc(id), cq(id, cc, cq_entries, 256, 0), running(true),
      heap(52U, size2Class, *this) {
  assert(max_wr);

  check_zero(rdma_migrate_id(id.get(), ec.get()));

  ibv_srq_init_attr srq_attr = { nullptr, { max_wr, 1, 0 } };
  check_zero(rdma_create_srq(id.get(), nullptr, &srq_attr));

  log_info() << "Created id " << id.get() << " " << (void *)this;
  hosts.pop_back();

  for (const auto &host : hosts) {
    ibv_qp_init_attr attr = {};
    attr.cap.max_send_wr = 4;
    attr.cap.max_recv_wr = 4;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.recv_cq = cq;
    attr.send_cq = cq;
    attr.srq = id->srq;
    attr.cap.max_inline_data = 72;
    attr.sq_sig_all = 1;
    auto id = createCmId(host, port, true, &attr);

    check_zero(rdma_migrate_id(id.get(), ec.get()));

    ids.push_back(std::move(id));

    log_info() << srq_attr;
    cq_entries_ = srq_attr.attr.max_wr;
  }

  cm_events();
}

RDMAServerSocket::~RDMAServerSocket() {
  running = false;
  for (auto &&id : ids) {
    rdma_disconnect(id.get());
  }
  rdma_disconnect(id.get());
  rdma_destroy_srq(id.get());
}

void RDMAServerSocket::disconnect(const qp_t qp_num) const {
  return clients([=](auto &clients) {
    auto client = clients.find(qp_num);
    if (client != std::end(clients)) {
      rdma_disconnect(client->second.get());
      clients.erase(client);
    } else {
      std::ostringstream s;
      s << "rdma_cm_id* for qp " << qp_num << " not found." << std::endl;
      throw std::runtime_error(s.str());
    }
  });
}

rdma_cm_id *RDMAServerSocket::find(const qp_t qp_num) const {
  return clients([=](const auto & clients)->rdma_cm_id * {
    auto client = clients.find(qp_num);
    if (client != std::end(clients))
      return client->second.get();
    else
      return nullptr;
  });
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
  qp_attr.cap.max_send_wr = 10;
  qp_attr.cap.max_recv_wr = 10;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 72;
  qp_attr.recv_cq = cq;
  qp_attr.send_cq = cq;
  qp_attr.srq = id->srq;
  qp_attr.sq_sig_all = 1;

  check_zero(rdma_create_qp(client_id.get(), NULL, &qp_attr));

  check_zero(rdma_accept(client_id.get(), nullptr));

  clients([client_id = std::move(client_id)](auto & clients) mutable {
    clients.emplace(client_id->qp->qp_num, std::move(client_id));
  });
}

void RDMAServerSocket::cm_events() const {
  eventThread.send([=]() {
    hydra::util::epoll poll;
    epoll_event poll_event;
    poll_event.events = EPOLLIN;
    poll_event.data.fd = id->channel->fd;

    poll.add(id->channel->fd, &poll_event);

    while (running.load()) {
      rdma_cm_event *cm_event = nullptr;

      if (id->event) {
        rdma_ack_cm_event(id->event);
        id->event = nullptr;
      }

      int ret = poll.wait(&poll_event, 1, -1);
      if (ret) {
        if (poll_event.data.fd == id->channel->fd) {
          check_zero(rdma_get_cm_event(id->channel, &cm_event));
          check_zero(cm_event->status);
        } else {
          log_err() << "Unkown fd " << poll_event.data.fd << " set. Expected "
                    << id->channel->fd;
          std::terminate();
        }
      }

      if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        accept(client_t(cm_event->id));
      } else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
        rdma_disconnect(cm_event->id);
      }
    }
  });
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


