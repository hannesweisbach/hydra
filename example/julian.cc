#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <iso646.h>

#define BUFFER_SIZE 4096

static bool is_client;

struct conn {
  struct rdma_cm_id *id;
  struct ibv_qp     *qp;
  struct ibv_mr     *recv_mr;
  struct ibv_mr     *send_mr;

  char *recv_region;
  char *send_region;
};

struct context {
  struct ibv_context      *ctx;
  struct ibv_pd           *pd;
  struct ibv_cq           *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_thread;
};

static struct context *ctx;


static void *
poll_cq(void *opaque)
{
  struct context *ctx = static_cast<struct context *>(opaque);

  printf("Poll thread up.\n");

  while (true) {
    struct ibv_cq  *cq;
    struct ibv_wc   wc;
    void           *cq_ctx;

    
    if (ibv_get_cq_event(ctx->comp_channel, &cq, &cq_ctx)) {
      perror("ibv_get_cq_event");
      break;
    }
    ibv_ack_cq_events(cq, 1);

    if (ibv_req_notify_cq(cq, 0)) {
      perror("ibv_req_notify_cq");
      break;
    }
    
    while (ibv_poll_cq(cq, 1, &wc)) {
      if (wc.status != IBV_WC_SUCCESS) {
	printf("status is not IBV_WC_SUCCESS: %u opcode %u\n", wc.status, wc.opcode);
	goto done;
      }

      struct conn *conn = (struct conn *)(uintptr_t)wc.wr_id;
      if (wc.opcode & IBV_WC_RECV) {
	printf("cq: received: %s\n", conn->recv_region);
	rdma_disconnect(conn->id);
      } else if (wc.opcode == IBV_WC_SEND) {
	printf("cq: send completed.\n");
	rdma_disconnect(conn->id);
      } else {
	printf("cq: %x\n", wc.opcode);
      }
    }
  }

 done:
  printf("Poll thread dead.\n");
  return NULL;
}

static void
build_context(struct ibv_context *verbs)
{
  if (ctx) {
    assert(ctx->ctx == verbs);
    return;
  }
  ctx = static_cast<struct context *>(calloc(1, sizeof(*ctx)));
  ctx->ctx = verbs;
  
  ctx->pd = ibv_alloc_pd(verbs);
  if (ctx->pd == NULL) {
    perror("ibv_alloc_pd");
    exit(EXIT_FAILURE);
  }

  ctx->comp_channel = ibv_create_comp_channel(verbs);
  ctx->cq           = ibv_create_cq(ctx->ctx, 10, NULL, ctx->comp_channel, 0);

  if (not (ctx->comp_channel and ctx->cq)) {
    perror("context");
    exit(EXIT_FAILURE);
  }

  if (ibv_req_notify_cq(ctx->cq, 0)) {
    perror("ibv_req_notify_cq");
    exit(EXIT_FAILURE);
  }

  pthread_create(&ctx->cq_thread, NULL, poll_cq, ctx);
}

static void
build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));
  qp_attr->send_cq = ctx->cq;
  qp_attr->recv_cq = ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC; /* reliable connection-oriented */

  /* Negotiate minimum capabilities. */
  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1; /* 1 scatter-gather element per send/recv */
  qp_attr->cap.max_recv_sge = 1;
}

static void
register_memory(struct conn *conn)
{
  conn->send_region = static_cast<char *>(calloc(1, BUFFER_SIZE));
  conn->recv_region = static_cast<char *>(calloc(1, BUFFER_SIZE));

  conn->send_mr = ibv_reg_mr(ctx->pd, conn->send_region, BUFFER_SIZE,
			     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  conn->recv_mr = ibv_reg_mr(ctx->pd, conn->recv_region, BUFFER_SIZE,
			     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  
  if (not (conn->send_mr and conn->recv_region)) {
    printf("Memory registration failed.\n");
    exit(EXIT_FAILURE);
  }
}

static void
post_receives(struct conn *conn)
{
  struct ibv_recv_wr  wr;
  struct ibv_recv_wr *bad_wr = NULL;
  struct ibv_sge      sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next  = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr   = (uintptr_t)conn->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey   = conn->recv_mr->lkey;

  if (ibv_post_recv(conn->qp, &wr, &bad_wr)) {
    perror("ibv_post_recv");
    exit(EXIT_FAILURE);
  }
}

static bool
on_event_ADDR_RESOLVED(struct rdma_cm_event *event)
{
  printf("Address resolved.\n");
  if (is_client) {
    /* We have a valid context pointer. Start building stuff. */
    struct ibv_qp_init_attr qp_attr;

    build_context(event->id->verbs);
    build_qp_attr(&qp_attr);

    if (rdma_create_qp(event->id, ctx->pd, &qp_attr)) {
      perror("rdma_create_qp");
      return true;
    }

    struct conn *conn;
    event->id->context = conn = static_cast<struct conn *>(calloc(1, sizeof(*conn)));
    conn->id = event->id;
    conn->qp = event->id->qp;

    register_memory(conn);

    /* We don't receive anything. */
    //post_receives(conn);

    if (rdma_resolve_route(event->id, 10000 /* ms */)) {
      perror("rdma_resolve_route");
      return true;
    }

    return false;
  } else {
    /* Server */
    printf("%s: not implemented\n", __func__);
    return true;
  }
}

static bool
on_event_ROUTE_RESOLVED(struct rdma_cm_event *event)
{
  printf("Route resolved.\n");
  if (is_client) {
    if (rdma_connect(event->id, NULL)) {
      perror("rdma_connect");
      return true;
    }

    /* Now? */

    return false;
  } else {
    /* Server */
    printf("%s: not implemented\n", __func__);
    return true;
  }
}

static bool
on_event_CONNECT_REQUEST(struct rdma_cm_event *event)
{
  if (is_client) {
    printf("%s: not implemented\n", __func__);
    return true;
  } else {
    /* Server */

    struct ibv_qp_init_attr qp_attr;
    build_context(event->id->verbs);
    build_qp_attr(&qp_attr);

    if (rdma_create_qp(event->id, ctx->pd, &qp_attr)) {
      perror("rdma_create_qp");
      return true;
    }

    struct conn *conn;
    event->id->context = conn = static_cast<struct conn *>(calloc(1, sizeof(*conn)));
    conn->id = event->id;
    conn->qp = event->id->qp;

    register_memory(conn);
    post_receives(conn);

    printf("Accepting connection.\n");
    if (rdma_accept(event->id, NULL)) {
      perror("rdma_accept");
      return true;
    }

    return false;
  }
}

static bool
on_event_ESTABLISHED(struct rdma_cm_event *event)
{
  printf("Connection established.\n");
  if (is_client) {
    struct conn *conn = static_cast<struct conn *>(event->id->context);
    struct ibv_send_wr  wr;
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge      sge;
    
    memset(&wr, 0, sizeof(wr));

    wr.wr_id   = (uintptr_t)conn;
    wr.next    = NULL;
    wr.opcode  = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr   = (uintptr_t)conn->send_region;
    sge.length = BUFFER_SIZE;
    sge.lkey   = conn->send_mr->lkey;

    strncpy(conn->send_region, "Hello World!", BUFFER_SIZE);

    if (ibv_post_send(conn->qp, &wr, &bad_wr)) {
      perror("ibv_post_send");
      return true;
    }

  } else {
    /* Do nothing. */
  }

  return false;
}

static bool
on_event_DISCONNECTED(struct rdma_cm_event *event)
{
  struct conn *conn = static_cast<struct conn *>(event->id->context);

  printf("Disconnecting.\n");

  rdma_destroy_qp(event->id);
  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);
  free(conn);
  rdma_destroy_id(event->id);

  return true;
}

static bool
on_event_CONNECT_ERROR(struct rdma_cm_event *event)
{
  printf("%s: not implemented\n", __func__);
  return true;
}

static bool
on_event_ROUTE_ERROR(struct rdma_cm_event *event)
{
  printf("%s: not implemented\n", __func__);
  return true;
}

static bool
on_event_REJECTED(struct rdma_cm_event *event)
{
  printf("%s: not implemented\n", __func__);
  return true;
}

static bool
on_event(struct rdma_cm_event *event)
{

#define HANDLE(_event) case RDMA_CM_EVENT_ ## _event: \
  return on_event_ ## _event (event)

  switch (event->event) {
    HANDLE(ADDR_RESOLVED);
    HANDLE(ROUTE_RESOLVED);
    HANDLE(CONNECT_REQUEST);
    HANDLE(ESTABLISHED);
    HANDLE(DISCONNECTED);
    HANDLE(CONNECT_ERROR);
    HANDLE(ROUTE_ERROR);
    HANDLE(REJECTED);
  default:
    printf("Unknown event %u\n", event->event);
    return true;
  }
}

int main(int argc, char **argv)
{
  struct rdma_event_channel *ec  = NULL;
  struct rdma_cm_event *event    = NULL;
  struct rdma_cm_id    *id       = NULL;

  if ((ec = rdma_create_event_channel()) == NULL) { 
    perror("rdma_create_event_channel");
    return EXIT_FAILURE;
  }

  if ((argc < 2) or (argc > 3)) {
    fprintf(stderr, "Usage: server-ip server-port (client mode)\n"
	    "       bind-ip               (server mode)\n");
    return EXIT_FAILURE;
  }

  if (argc == 3) {
    /* *** CLIENT *** */
    is_client = true;

    struct addrinfo *addr;
    int err;
    if ((err = getaddrinfo(argv[1], argv[2], NULL, &addr))) {
      printf("getaddrinfo: %s\n", gai_strerror(err));
      return EXIT_FAILURE;
    }

    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) {
      perror("rdma_create_id");
      return EXIT_FAILURE;
    }

    if (rdma_resolve_addr(id, NULL, addr->ai_addr, 10000 /* ms */)) {
      perror("rdma_resolve_addr");
      return EXIT_FAILURE;
    }
    freeaddrinfo(addr);

  } else {
    /* *** SERVER *** */
    is_client = false;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = 10023;
    addr.sin_addr.s_addr = inet_addr(argv[1]);

    uint16_t port = 0;

    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) {
      perror("rdma_create_id");
      return EXIT_FAILURE;
    }

    if (rdma_bind_addr(id, (struct sockaddr *)&addr)) {
      perror("rdma_bind_addr");
      return EXIT_FAILURE;
    }

    if (rdma_listen(id, 10)) {
      perror("rdma_listen");
      return EXIT_FAILURE;
    }

    port = ntohs(rdma_get_src_port(id));
    printf("Got port %u.\n", port);
  
  }

  printf("Entering %s event loop.\n", is_client ? "client" : "server");
  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;
    event_copy = *event;
    rdma_ack_cm_event(event);
    
    if (on_event(&event_copy)) break;
  }


  rdma_destroy_id(id);
  rdma_destroy_event_channel(ec);

  return 0;
}
