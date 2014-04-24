#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <iterator>
#include <exception>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <netdb.h>
#include <unistd.h>

#include "RDMAWrapper.hpp"

#include "util/concurrent.h"

#define CASE(_case_)                                                           \
  case _case_:                                                                 \
    return ostream << #_case_

std::ostream &operator<<(std::ostream &ostream,
                         const enum ibv_wc_status &status) {
  return ostream << ibv_wc_status_str(status);
}

std::ostream &operator<<(std::ostream &ostream,
                         const enum rdma_cm_event_type &type) {
  return ostream << rdma_event_str(type);
}

std::ostream &operator<<(std::ostream &ostream,
                         const enum ibv_wc_opcode &opcode) {
  switch (opcode) {
    CASE(IBV_WC_SEND);
    CASE(IBV_WC_RDMA_WRITE);
    CASE(IBV_WC_RDMA_READ);
    CASE(IBV_WC_COMP_SWAP);
    CASE(IBV_WC_FETCH_ADD);
    CASE(IBV_WC_BIND_MW);
    CASE(IBV_WC_RECV);
    CASE(IBV_WC_RECV_RDMA_WITH_IMM);
  }
#if __GNUC__ and not __clang__
  return ostream;
#endif
}

std::ostream &operator<<(std::ostream &ostream, const ibv_mr &mr) {
  ostream << "ibv_mr {" << std::endl;
  {
    indent_guard guard(ostream);
    ostream << indent << "struct ibv_context *context = " << (void *)mr.context
            << std::endl;
    ostream << indent << "struct ibv_pd *pd = " << (void *)mr.pd << std::endl;
    ostream << indent << "void *addr = " << mr.addr << std::endl;
    ostream << indent << "size_t length = " << mr.length << std::endl;
    ostream << indent << "uint32_t handle = " << mr.handle << std::endl;
    ostream << indent << "uint32_t lkey = " << mr.lkey << std::endl;
    ostream << indent << "uint32_t rkey = " << mr.rkey << std::endl;
  }
  ostream << indent << "}; @" << (void *)&mr << std::endl;
  return ostream;
}

std::ostream &operator<<(std::ostream &ostream, const enum ibv_atomic_cap& cap) {
  switch (cap) {
    CASE(IBV_ATOMIC_NONE);
    CASE(IBV_ATOMIC_HCA);
    CASE(IBV_ATOMIC_GLOB);
  }
#if __GNUC__ and not __clang__
  return ostream;
#endif
}

std::ostream &operator<<(std::ostream &ostream, const ibv_device_attr &attr) {
  ostream << "struct ibv_device_attr {" << std::endl;
  {
    indent_guard guard(ostream);
    ostream << indent << "char fw_ver[64] = " << attr.fw_ver << std::endl;
    ostream << indent << "uint64_t node_guid = " << attr.node_guid << std::endl;
    ostream << indent << "uint64_t sys_image_guid = " << attr.sys_image_guid
            << std::endl;
    ostream << indent << "uint64_t max_mr_size = " << attr.max_mr_size
            << std::endl;
    ostream << indent << "uint64_t page_size_cap = " << attr.page_size_cap
            << std::endl;
    ostream << indent << "uint32_t vendor_id = " << attr.vendor_id << std::endl;
    ostream << indent << "uint32_t vendor_part_id = " << attr.vendor_part_id
            << std::endl;
    ostream << indent << "uint32_t hw_ver = " << attr.hw_ver << std::endl;
    ostream << indent << "int max_qp = " << attr.max_qp << std::endl;
    ostream << indent << "int max_qp_wr = " << attr.max_qp_wr << std::endl;
    ostream << indent << "int device_cap_flags = " << std::hex
            << attr.device_cap_flags << std::dec << std::endl;
    ostream << indent << "int max_sge = " << attr.max_sge << std::endl;
    ostream << indent << "int max_sge_rd = " << attr.max_sge_rd << std::endl;
    ostream << indent << "int max_cq = " << attr.max_cq << std::endl;
    ostream << indent << "int max_cqe = " << attr.max_cqe << std::endl;
    ostream << indent << "int max_mr = " << attr.max_mr << std::endl;
    ostream << indent << "int max_pd = " << attr.max_pd << std::endl;
    ostream << indent << "int max_qp_rd_atom = " << attr.max_qp_rd_atom
            << std::endl;
    ostream << indent << "int max_ee_rd_atom = " << attr.max_ee_rd_atom
            << std::endl;
    ostream << indent << "int max_res_rd_atom = " << attr.max_res_rd_atom
            << std::endl;
    ostream << indent
            << "int max_qp_init_rd_atom = " << attr.max_qp_init_rd_atom
            << std::endl;
    ostream << indent
            << "int max_ee_init_rd_atom = " << attr.max_ee_init_rd_atom
            << std::endl;
    ostream << indent << "enum ibv_atomic_cap atomic_cap = " << attr.atomic_cap
            << std::endl;
    ostream << indent << "int max_ee = " << attr.max_ee << std::endl;
    ostream << indent << "int max_rdd = " << attr.max_rdd << std::endl;
    ostream << indent << "int max_mw = " << attr.max_mw << std::endl;
    ostream << indent << "int max_raw_ipv6_qp = " << attr.max_raw_ipv6_qp
            << std::endl;
    ostream << indent << "int max_raw_ethy_qp = " << attr.max_raw_ethy_qp
            << std::endl;
    ostream << indent << "int max_mcast_grp = " << attr.max_mcast_grp
            << std::endl;
    ostream << indent
            << "int max_mcast_qp_attach = " << attr.max_mcast_qp_attach
            << std::endl;
    ostream << indent << "int max_total_mcast_qp_attach = "
            << attr.max_total_mcast_qp_attach << std::endl;
    ostream << indent << "int max_ah = " << attr.max_ah << std::endl;
    ostream << indent << "int max_fmr = " << attr.max_fmr << std::endl;
    ostream << indent << "int max_map_per_fmr = " << attr.max_map_per_fmr
            << std::endl;
    ostream << indent << "int max_srq = " << attr.max_srq << std::endl;
    ostream << indent << "int max_srq_wr = " << attr.max_srq_wr << std::endl;
    ostream << indent << "int max_srq_sge = " << attr.max_srq_sge << std::endl;
    ostream << indent << "uint16_t max_pkeys = " << attr.max_pkeys << std::endl;
    ostream << indent << "uint8_t local_ca_ack_delay = "
            << (unsigned int)attr.local_ca_ack_delay << std::endl;
    ostream << indent
            << "uint8_t phys_port_cnt = " << (unsigned int)attr.phys_port_cnt
            << std::endl;
  }
  ostream << indent << "}; @" << (void *)&attr;
  return ostream;
}

std::ostream &operator<<(std::ostream &ostream, const ibv_device &dev) {
  ostream << "struct ibv_device {" << std::endl;
  {
    indent_guard guard(ostream);
    ostream << indent << "struct ibv_device_ops ops = " << (void *)&dev.ops << std::endl;
    ostream << indent << "enum ibv_node_type node_type = " << dev.node_type << std::endl;
    ostream << indent << "enum ibv_transport_type transport_type = " << dev.transport_type
            << std::endl;
    ostream << indent << "char name = " << dev.name << std::endl;
    ostream << indent << "char dev_name = " << dev.dev_name << std::endl;
    ostream << indent << "char dev_path = " << dev.dev_path << std::endl;
    ostream << indent << "char ibdev_path = " << dev.ibdev_path << std::endl;
  }
  ostream << indent <<  "}; @" << (void *)&dev;
  return ostream;
}

std::ostream &operator<<(std::ostream &ostream, const ibv_srq_init_attr &attr) {
  ostream << "struct ibv_srq_init_attr {" << std::endl;
  {
    indent_guard guard(ostream);
    ostream << indent << "void *srq_context = " << attr.srq_context
            << std::endl;
    ostream << indent << "ibv_srq_attr attr = " << attr.attr << std::endl;
  }
  ostream << indent << "}; @" << (void *)&attr;
  return ostream;
}

std::ostream &operator<<(std::ostream &ostream, const ibv_srq_attr &attr) {
  ostream << "struct ibv_srq_attr {" << std::endl;
  {
    indent_guard guard(ostream);
    ostream << indent << "uint32_t max_wr    = " << attr.max_wr << std::endl;
    ostream << indent << "uint32_t max_sge   = " << attr.max_sge << std::endl;
    ostream << indent << "uint32_t srq_limit = " << attr.srq_limit << std::endl;
  }
  ostream << indent << "}; @" << (void *)&attr;
  return ostream;
}

ec_ptr createEventChannel() {
  return ec_ptr(check_nonnull(::rdma_create_event_channel()),
                ::rdma_destroy_event_channel);
}

class AddrIterator {
  rdma_addrinfo *node;

public:
  AddrIterator(rdma_addrinfo *node_) noexcept : node(node_) {}
  rdma_addrinfo *&operator*() { return node; }

  rdma_addrinfo **operator->(){ return &node; }

  AddrIterator &operator++() {
    node = node->ai_next;
    return *this;
  }

  AddrIterator operator++(int) {
    AddrIterator tmp = *this;
    node = node->ai_next;
    return tmp;
  }

  bool operator==(const AddrIterator &other) const {
    return node == other.node;
  }
  bool operator!=(const AddrIterator &other) const {
    return node != other.node;
  }
};

class AddrList {
  std::unique_ptr<rdma_addrinfo, decltype(&rdma_freeaddrinfo)> list;

public:
  AddrList(const std::string &host, const std::string &port, const bool passive)
      : list([&]() {
               char *hostname =
                   (host.empty()) ? nullptr : const_cast<char *>(host.c_str());

               rdma_addrinfo *ai = nullptr;
               rdma_addrinfo hints = {};
               if (passive)
                 hints.ai_flags = RAI_PASSIVE;
               hints.ai_port_space = RDMA_PS_TCP;

               check_zero(rdma_getaddrinfo(
                   hostname, const_cast<char *>(port.c_str()), &hints, &ai));
               return check_nonnull(ai);
             }(),
             ::rdma_freeaddrinfo) {}
  AddrIterator begin() { return AddrIterator(list.get()); }
  AddrIterator end() { return AddrIterator(nullptr); }
};

AddrIterator begin(AddrList &addr_list) { return addr_list.begin(); }
AddrIterator end(AddrList &addr_list) { return addr_list.end(); }

namespace std {
template <> AddrIterator begin<AddrList>(AddrList &addr_list) {
  return addr_list.begin();
}
template <> AddrIterator end<AddrList>(AddrList &addr_list) {
  return addr_list.end();
}
}

static void debug_deleter (rdma_cm_id *) {
  assert(false);
}
rdma_id_ptr createCmId(const std::string &host, const std::string &port,
                       const bool passive, ibv_qp_init_attr *attr) {
  std::exception_ptr exception;

  for (auto ai : AddrList(host, port, passive)) {
    if(ai->ai_src_canonname || ai->ai_dst_canonname)
      log_info() << "Connect from " << ai->ai_src_canonname << " to "
                 << ai->ai_dst_canonname;

    char iface[NI_MAXHOST];
    if (!getnameinfo(ai->ai_src_addr, ai->ai_src_len, iface, sizeof(iface),
                     nullptr, 0, NI_NUMERICHOST)) {
      log_debug() << "Found address " << iface << " " << (void *)ai;
    }

    try {
      ::rdma_cm_id *id;
      check_zero(::rdma_create_ep(&id, ai, nullptr, attr));
      return rdma_id_ptr(id,
#if 1
                         ::rdma_destroy_ep
#else
                         debug_deleter
#endif
                         );
    }
    catch (const std::exception &e) {
      log_err() << e.what();
      exception = std::current_exception();
    }
  }

  std::ostringstream s;
  s << "Error connecting to  " << host << ":" << port;
  try {
    if (exception != std::exception_ptr())
      std::rethrow_exception(exception);
  }
  catch (const std::exception &e) {
    s << ": " << e.what();
  }

  throw std::runtime_error(s.str());
}

[[deprecated]] std::shared_ptr< ::ibv_pd>
               createProtectionDomain(std::shared_ptr< ::rdma_cm_id> id) {
  ::ibv_pd *pd = check_nonnull(::ibv_alloc_pd(id.get()->verbs));
  return std::shared_ptr< ::ibv_pd>(pd, ::ibv_dealloc_pd);
}

comp_channel_ptr createCompChannel(rdma_id_ptr &id) {
  ::ibv_comp_channel *channel =
      check_nonnull(::ibv_create_comp_channel(id->verbs));
  return comp_channel_ptr(channel, ::ibv_destroy_comp_channel);
}

cq_ptr createCQ(rdma_id_ptr &id, int entries, void *context,
                comp_channel_ptr &channel, int completion_vector) {
  ::ibv_cq *cq = check_nonnull(::ibv_create_cq(
      id.get()->verbs, entries, context, channel.get(), completion_vector));
  return cq_ptr(cq, ::ibv_destroy_cq);
}

static void call_completion_handler(const ibv_wc &wc) {
  /* wc.wr_id is a pointer to the continuation handler */
  auto f = reinterpret_cast<std::function<void(const ibv_wc &)> *>(wc.wr_id);
  if (f) {
    // TODO dispatch-async: ?
    (*f)(wc);
  } else {
    log_info() << wc.opcode << ": No completion handler registered";
  }
}

static bool recv_helper__(ibv_comp_channel *cc) {
  int ret;
  unsigned int n_events = 0;
  void *user_context;
  struct ibv_cq *cq;
  struct ibv_wc wcs[16];
  bool flushing = false;

  /* TODO maybe use rdma_get_recv/send_comp? */
  check_zero(ibv_get_cq_event(cc, &cq, &user_context));
  ibv_ack_cq_events(cq, 1);
  check_zero(ibv_req_notify_cq(cq, 0));

  while ((ret = ibv_poll_cq(cq, 16, wcs))) {
    if (ret < 0) {
      throw_errno("ibv_poll_cq()");
      break;
    }

    for (int i = 0; i < ret; i++) {
      struct ibv_wc wc = wcs[i];
      if (wc.status == IBV_WC_WR_FLUSH_ERR) {
        flushing = true;
      }

      if (wc.status != IBV_WC_SUCCESS) {
        /* wc.opcode is invalid - but we need to set the an exception in the
         * std::future */
        call_completion_handler(wc);
        continue;
      }

      switch (wc.opcode) {
      case IBV_WC_SEND:
        [[clang::fallthrough]];
      case IBV_WC_RECV:
        [[clang::fallthrough]];
      case IBV_WC_RDMA_READ:
        call_completion_handler(wc);
        break;
      default:
        log_info() << "Received " << wc.opcode << " on id " << std::hex
                   << std::showbase << wc.wr_id << std::dec;
        break;
      }
    }
  }

  return flushing;
}

class epoll_wrapper {
  int efd;

public:
  epoll_wrapper() : efd(epoll_create(1)) {
    if (efd < 0)
      throw_errno();
  }
  ~epoll_wrapper() { close(efd); }
  void add(int fd, epoll_event *event) {
    check_zero(epoll_ctl(efd, EPOLL_CTL_ADD, fd, event));
  }
  void del(int fd) { check_zero(epoll_ctl(efd, EPOLL_CTL_DEL, fd, nullptr)); }
  void mod(int fd, epoll_event *event) {
    check_zero(epoll_ctl(efd, EPOLL_CTL_MOD, fd, event));
  }
  int wait(epoll_event *events, int maxevents, int timeout) {
    for (;;) {
      int ret = epoll_wait(efd, events, maxevents, timeout);
      if (ret < 0) {
        if (errno == EINTR) {
          log_info() << "Interrupted.";
          continue;
        } else {
          throw_errno();
        }
      }
      return ret;
    }
  }
};

std::future<void> rdma_handle_cq_event_async(ibv_comp_channel *cc,
                                             async_queue_type queue,
                                             int pipe_fd) {
  return hydra::async(queue, [=] {
    std::array<epoll_event, 2> events;
    epoll_wrapper poll;
    events[0].events = EPOLLIN;
    events[0].data.fd = cc->fd;
    events[1].events = EPOLLIN;
    events[1].data.fd = pipe_fd;

    poll.add(cc->fd, &events[0]);
    poll.add(pipe_fd, &events[1]);

    bool flushing = false;
    while (1) {
      /* TODO: throw exeception/return if
       * - we got IBV_WC_WR_FLUSH_ERR (i.e. QP is in error state --
       *   rdma_disconnect() was called) and
       * - subsequent select() timed out (we got the last event -- the queue is
       *   drained
       *   TODO: we don't have a timeout anymore. can we just quit? (and thus
       *         drop flushing?)
       * - The above doesn't work for an (empty) send queue.
       *   -> use SRQ?
       *   -> boolean
       */
      int ret = poll.wait(events.data(), events.size(), -1);
      for (int i = 0; i < ret; i++) {
        if (events[i].data.fd == cc->fd) {
          flushing = recv_helper__(cc);
        } else if (events[i].data.fd == pipe_fd) {
          close(pipe_fd);
          return;
        } else {
          log_err() << "Quit select without error, timeout and an fd set";
          assert(false);
        }
      }
    }
  });
}

void dereg_debug(ibv_mr *mr) { log_err() << "deregging " << mr; }

class rdma_initializer {
public:
  rdma_initializer() {
    // TODO: add loop-back switch.
    std::vector<std::string> needed_modules(
        { /*"siw",*/ "ib_ipoib", "ib_uverbs", "ib_umad", "rdma_ucm", "ib_mthca" });
    std::ifstream modules("/proc/modules");
    while (modules.good()) {
      std::string module;
      std::string dummy;
      std::getline(modules, module);
      needed_modules.erase(
          std::remove_if(needed_modules.begin(), needed_modules.end(),
                         [&](std::string m) { return module.find(m) == 0; }),
          needed_modules.end());
    }

    for (std::string m : needed_modules) {
      std::string cmd("modprobe " + m);
      std::cout << "Loading module " << m << " … ";
      if (::system(cmd.c_str()))
        std::cout << "failed";
      else
        std::cout << "successful";
      std::cout << std::endl;
    }

    rlimit rlim = { RLIM_INFINITY, RLIM_INFINITY };
    if (setrlimit(RLIMIT_MEMLOCK, &rlim))
      std::cout << "Error: " << strerror(errno) << std::endl;

    if (getrlimit(RLIMIT_MEMLOCK, &rlim))
      std::cout << "Error: " << strerror(errno) << std::endl;

    std::cout << "Hard Limit: " << rlim.rlim_max << std::endl;
    std::cout << "Soft Limit: " << rlim.rlim_cur << std::endl;
  }
};

#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wglobal-constructors"
static rdma_initializer rdma_init;
#pragma clang diagnostic pop
#else
static rdma_initializer rdma_init;
#endif
