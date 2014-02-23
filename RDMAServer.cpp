#include <netdb.h>
#include <assert.h>
#include <iostream>
#include <thread>
#include <memory>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <assert.h>
#include <getopt.h>

#include "RDMAServer.hpp"
#include "RDMAWrapper.hpp"
#include "Logger.h"
#include "utils.h"

static const char msg[] = "This is the message.";

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

void handle(const void * mem, size_t len, uint64_t imm, bool imm_valid) {
  log_trace() << "Called" << std::endl;
  log_debug() << mem << " " << len << ((imm_valid) ? imm : 0);
}




RDMAServer::RDMAServer(const std::string& port) : RDMAServer(std::string(), port) {}

RDMAServer::RDMAServer(const std::string &host, const std::string &port)
    : 
      ec(createEventChannel())
  {
  /* either specified ip addr or nullptr (all interfaces) */
  const char * hostname = (host.empty()) ? nullptr : host.c_str();

  struct rdma_addrinfo *ais = nullptr;
  struct rdma_addrinfo hints;// = {};
  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = RAI_PASSIVE;
  hints.ai_port_space = RDMA_PS_TCP;

  int err = rdma_getaddrinfo((char *)hostname, (char*)port.c_str(), &hints, &ais);
  if(err)
    throw_errno("get_addr_info");

  for(struct rdma_addrinfo * ai = ais; ai != nullptr; ai = ai->ai_next) {
    log_info() << ai->ai_src_canonname << " " << ai->ai_dst_canonname;
    char iface[NI_MAXHOST];
    (void)getnameinfo(ai->ai_src_addr, ai->ai_src_len, iface, sizeof(iface), NULL,
                      0, NI_NUMERICHOST);
    log_debug() << "Found address " << iface << " " << (void*)ai;
    
#if 0
    if(rdma_bind_addr(id.get(), ai->ai_src_addr)) {
      log_error() << "Error binding to address " << strerror(errno);
      continue;
    } else {
      break;
    }
    ibv_qp_init_attr attr = { nullptr,             cq.get(),   cq.get(), nullptr,
                            { 10, 10, 1, 1, 0 }, IBV_QPT_RC, 0 };
#else
    struct rdma_cm_id * _id;
    if(rdma_create_ep(&_id, ai, nullptr, nullptr))
      throw_errno("rdma_create_ep");
    else {
      id = std::shared_ptr< ::rdma_cm_id>(_id, ::rdma_destroy_ep);
      break; /* ??? */
    }
#endif
  }
  rdma_freeaddrinfo(ais);
#if 0
  if(rdma_migrate_id(id.get(), ec.get()))
    throw_errno("rdma_migrate_id");
  
#endif

  struct ibv_srq_init_attr srq_attr;
  memset(&srq_attr, 0, sizeof (srq_attr));
  srq_attr.attr.max_wr = 10;
  srq_attr.attr.max_sge = 1;
  if(rdma_create_srq(id.get(), nullptr, &srq_attr))
    throw_errno("rdma_create_srq");

  /* doesn't compile no more */
  //cc = createCompChannel(id);

  //cq = createCQ(id, 10, nullptr, cc, 0);

  if(ibv_req_notify_cq(cq.get(), 0))
    throw_errno("ibv_req_notify");
  
  size_t size = 32*1024;
  for(ssize_t i = 0; i < 30; ++i) {
    snprintf(static_cast<char*>(allocMapMemory(size)), size, "This is memory %zd\n", i);
  }

  if(rdma_listen(id.get(), 10))
    throw_errno("rdma_listen");
  
  log_info() << "Started server on port " << port;
}

void *
RDMAServer::allocMapMemory(size_t size, size_t alignment) {
  mem.emplace_back(RDMAReadMem(id, size, alignment));
  return mem.back().get();
}

void
RDMAServer::post_send(rdma_cm_id* id) {
  int err;
  const size_t size = sizeof(msg) + sizeof(ibv_mr);
  void * ptr = malloc(size);
  memcpy(ptr, msg, sizeof(msg));
  memcpy((char*)ptr + sizeof(msg), mem.front().mr().get(), sizeof(ibv_mr));
  ibv_mr *mr =
      check_nonnull(ibv_reg_mr(id->pd, ptr, size, ::IBV_ACCESS_REMOTE_READ));
  // std::shared_ptr< ::ibv_mr> mr =
  //    registerMemory(ev->id->pd/*pd*/, msg, ::IBV_ACCESS_REMOTE_READ);
  ibv_sge sge = {(uint64_t)ptr, (uint32_t)size, mr->lkey };
  ibv_send_wr wr;// = {};
  memset(&wr, 0, sizeof(wr));
  ibv_send_wr *bad_wr = nullptr;
  wr.wr_id = reinterpret_cast<uint64_t>(mr);
  wr.opcode = ::IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  
  err = ::ibv_post_send(id->qp, &wr, &bad_wr);
  if (err)
    log_error() << "ibv_post_send " << strerror(errno);
}

void RDMAServer::run() {
  log_info() << "Running";

#if 0
  ibv_qp_init_attr attr = { nullptr,             cq.get(),   cq.get(), nullptr,
                            { 10, 10, 1, 1, 0 }, IBV_QPT_RC, 0 };
#endif
  while (1) {
#if 1
    log_trace() << "Waiting for request";
    
    struct rdma_cm_id * clientId;
    if(rdma_get_request(id.get(), &clientId))
      throw_errno("rdma_get_request");
    
    log_trace() << "Connection request";

#if 0
    if(rdma_reg_read(clientId, malloc(1024*1024*10), 1024*1024*10))
      throw_errno("rdma_reg_read 10");
    if(rdma_reg_read(clientId, malloc(1024*1024*100), 1024*1024*100))
      throw_errno("rdma_reg_read 100");
#endif

    ibv_qp_init_attr qp_attr = {};
    //qp_attr.qp_context = ctx;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 10;
    qp_attr.cap.max_recv_wr = 10;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.recv_cq = cq.get();
    qp_attr.srq = id->srq;
    qp_attr.sq_sig_all = 0;
    int ret = rdma_create_qp(clientId, NULL, &qp_attr);
    if (ret)
      throw_errno("rdma_create_qp");
    /* Set the new connection to use our SRQ */
    clientId->srq = id->srq;


    if(rdma_accept(clientId, nullptr))
      throw_errno("rdma_accept");

    log_trace() << "Accepted Connection request";

    clients.emplace_back(clientId, rdma_destroy_id);
   
    if(id.get()->event) {
      log_debug() << "Acking existing event";
      rdma_ack_cm_event(id.get()->event);
      id.get()->event = nullptr;
    }

    post_send(clientId);

#if 0
    /* let's see if we get an disconnect event */
    log_trace() << clientId->channel->fd;
    struct rdma_cm_event *ev;
    rdma_get_cm_event(id.get()->channel, &ev);
    //rdma_get_cm_event(clientId->channel, &ev);
    log_trace() << "Got " << ev->event;
#endif

#else    
    int err;
 
    if(id.get()->event) {
      log_debug() << "Acking existing event";
      rdma_ack_cm_event(id.get()->event);
      id.get()->event = nullptr;
    }

    log_debug() << ec.get() << " " << id.get()->channel;
    rdma_cm_event *ev;
    rdma_cm_event event;
    //if(rdma_get_cm_event(ec.get(), &ev))
    if(rdma_get_cm_event(id.get()->channel, &ev))
      throw_errno("rdma_get_cm_event");

    event = *ev;
    rdma_ack_cm_event(ev);

    if(event.status)
      log_error() << "Event error " << event.status;

    log_trace() << "Got " << event.event;
    
    switch (event.event) {
    case RDMA_CM_EVENT_CONNECT_REQUEST: {
      ibv_qp_init_attr attr = { nullptr, cq.get(),            cq.get(),
                                id.get()->srq, { 10, 10, 1, 1, 0 }, IBV_QPT_RC,
                                1};
      err = rdma_create_qp(event.id, id.get()->pd, &attr);
      if (err) {
        log_error() << "rdma_create_qp: " << strerror(errno);
      }

      log_trace() << "Adding client " << event.id;
      clients.emplace_back(std::shared_ptr<rdma_cm_id>{event.id, rdma_destroy_id});
      rdma_accept(event.id, nullptr);
      break;
    }
    case RDMA_CM_EVENT_ESTABLISHED: {
      log_info() << "PD: " << event.id->pd;
#if 0
      /* align mem to page size */
      size_t failures = 0;
      for (size_t size = 8; size; size<<=1) {
        void * ptr;
#if 0
        ptr = malloc(size);
        
        if(ptr == nullptr)
          throw_errno("malloc");
#else
        err = posix_memalign(&ptr, size, 0xffff);
        if(err)
          throw_errno("posix_memalign");
#endif
        struct ibv_mr *mr = rdma_reg_read(event.id, ptr, size);
        if (mr != nullptr) {
          log_debug() << "Reg'ed " << size << " " << ptr;
          rdma_dereg_mr(mr);
        } else {
          log_error() << "Not able to reg " << size;
          failures++;
        }
        free(ptr);
        if (failures > 10)
          break;
      }
#endif
#if 0
      for (auto &m : mem) {

        m.mr = { rdma_reg_read(event.id, m.addr, m.size), rdma_dereg_mr };
        if(m.mr.get() == nullptr) {
          throw_errno("rdma_reg_read");
        }
        log_info() << m.mr.get()->addr << " " << m.mr.get()->length << " " << m.mr.get()->lkey << " " << m.mr.get()->rkey;
      }
#endif
      const size_t size = sizeof(msg) + sizeof(ibv_mr);
      void * ptr = malloc(size);
      memcpy(ptr, msg, sizeof(msg));
      memcpy((char*)ptr + sizeof(msg), mem.front().mr(), sizeof(ibv_mr));
      ibv_mr *mr =
          check_nonnull(ibv_reg_mr(event.id->pd, ptr, size, ::IBV_ACCESS_REMOTE_READ));
      // std::shared_ptr< ::ibv_mr> mr =
      //    registerMemory(ev->id->pd/*pd*/, msg, ::IBV_ACCESS_REMOTE_READ);
      ibv_sge sge = {(uint64_t)ptr, (uint32_t)size, mr->lkey };
      ibv_send_wr wr;// = {};
      memset(&wr, 0, sizeof(wr));
      ibv_send_wr *bad_wr = nullptr;
      wr.wr_id = reinterpret_cast<uint64_t>(mr);
      wr.opcode = ::IBV_WR_SEND;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;
      
      err = ::ibv_post_send(event.id->qp, &wr, &bad_wr);
      if (err)
        log_error() << "ibv_post_send " << strerror(errno);
      
      break;
    }
    case RDMA_CM_EVENT_DISCONNECTED: {
      log_trace() << "for client " << event.id;
      const auto &end = remove_if(clients.begin(), clients.end(),
                                  [=](std::shared_ptr<rdma_cm_id> client) {
        if(client.get() == event.id) {
          check_zero(rdma_disconnect(client.get()));
          //calls rdma_dereg_mr
          //mem[0].mr.reset();
          return true;
        } else {
          return false;
        }
        //return client.get() == event.id;
      });
      clients.erase(end, clients.end());
    }
    default:
      break;
    }
#endif
  }
}

void
RDMAServer::processEvents() {
  while(1) {
    int err;
    unsigned int n_events = 0;
    void * user_context;
    struct ibv_cq *cq;
    struct ibv_wc wc;

    err = ibv_get_cq_event(cc.get(), &cq, &user_context);
    if(err) {
      log_error() << "ibv_get_cq_event: " << strerror(errno);
    }

    while((err = ibv_poll_cq(cq, 1, &wc))) {
      if(err < 0) {
        log_error() << "ibv_poll_cq: " << strerror(errno);
        break;
      }
      n_events++;

      if(wc.status != IBV_WC_SUCCESS) {
        log_error() << "Error handling wc: " << wc.status << " (" << wc.opcode << ")";
      }
      
      switch(wc.opcode) {
        case IBV_WC_RECV: handle(nullptr, wc.byte_len, wc.imm_data, wc.wc_flags & IBV_WC_WITH_IMM);
        case IBV_WC_SEND: {
          log_info() << "Received " << wc.opcode
                                         << " which means we have to clean up.";
          struct ibv_mr * mr = reinterpret_cast<ibv_mr*>(wc.wr_id);
          free(mr->addr);
          if(ibv_dereg_mr(mr))
            log_error() << "Could not free MR " << *mr;

          break;
        }
        default: log_info() << "Received " << wc.opcode; break;
      }

    }

    ibv_ack_cq_events(cq, n_events);
    err = ibv_req_notify_cq(cq, 0);
    if(err) {
      log_error() << "ibv_req_notify_cq: " << strerror(errno);
    }

  }
}

static size_t
get_max_mr_size() {
  uint64_t min_mr_size = std::numeric_limits<uint64_t>::max();
  int num = 0;
  struct ibv_context ** devices_start;

  devices_start = rdma_get_devices(&num);

  log_info() << "Found " << num << " devices.";

  if(num == 0)
    return 0;

  for(struct ibv_context ** dev = devices_start; *dev; ++dev) {
    log_info() << *dev;
    struct ibv_device_attr attr;
    int err = ibv_query_device(*dev, &attr);
    if(err)
      continue;
    log_info() << attr;
    if(attr.max_mr_size < min_mr_size)
      min_mr_size = attr.max_mr_size;
  }

  rdma_free_devices(devices_start);

  log_info() << "Minimum MR size: " << min_mr_size;

  return min_mr_size;
}

static void
rdma_init_() {
  size_t rkey_size = sizeof(ibv_mr::rkey);
  size_t rdma_ptr_size = sizeof(ibv_mr::addr);
  size_t entry_size = rkey_size + rdma_ptr_size;

  size_t mr_size = get_max_mr_size();

  if(mr_size == 0) {
    log_fatal() << "MR size is 0, bailing out.";
    return;
  }

  size_t num_rkeys = mr_size / rkey_size;
  size_t page_bits = hydra::util::log2(num_rkeys);
  size_t page_align = page_bits;

  log_info() << num_rkeys << " " << page_bits;


  ssize_t ptr_size = std::numeric_limits<uintptr_t>::digits;
  log_info() << ptr_size;

  size_t total = std::numeric_limits<uintptr_t>::max();

  log_info() << total << " " << total - ((uintptr_t)1 << page_bits);

  log_info() << "ptr_size   " << ptr_size;
  log_info() << "page_align " << page_align;
  log_info() << "page_bits  " << page_bits;

  ptr_size -= page_align;
  ptr_size -= page_bits;


  while(ptr_size > 0) {
    size_t entries = mr_size / entry_size;
    if(entries == 0)
      break;
    
    size_t next_level_bits = hydra::util::log2(entries);

    size_t nlb;
    asm("bsrq %1,%0" : "=r"(nlb) : "r"(entries));
    assert(nlb == next_level_bits);

    log_info() << entries << " " << next_level_bits << " " << ptr_size << " " <<  ((uintptr_t)1 << ptr_size) - 1;
    ptr_size -= next_level_bits;
  }
}


int main(int argc, char * const argv[]) {
  static struct option long_options[] = {
    { "port",      required_argument, 0, 'p' },
    { "interface", required_argument, 0, 'i' },
    { "verbosity", optional_argument, 0, 'v' },
    { 0, 0, 0, 0 }
  };

  std::string host;
  std::string port("8042");
  int verbosity = 0;

  while (1) {
    int option_index = 0;
    int c = getopt_long(argc, argv, "p:i:", long_options, &option_index);

    if(c == -1)
      break;
    switch(c) {
      case 'p':
        port = optarg; break;
      case 'i':
        host = optarg; break;
      case 'v':
        if(optarg) {
          /* TODO: parse optarg for level */
        } else {
          verbosity++;
        }
      case '?':
      default:
       log_info() << "Unkown option code " << (char)c;
    }
  }

  Logger::set_severity(verbosity);
  rdma_init_();

  log_info() << "Starting on interface " << host << ":" << port;
  RDMAServer server(host, port);

  std::thread serverthread(&RDMAServer::run, &server);
  std::thread processing(&RDMAServer::processEvents, &server);

  serverthread.join();
  processing.join();

}
