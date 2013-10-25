#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <error.h>
#include <errno.h>
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

int const LISTEN_BACKLOG = 10;

void error (char const * msg) { perror (msg); exit (EXIT_FAILURE); }

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
poll_cq(void * opaque)
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

unsigned const ipv6 = 0;

int callback_ADDR_RESOLVED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ADDR_ERROR (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ROUTE_RESOLVED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ROUTE_ERROR (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_CONNECT_REQUEST (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);

    // --- julian ---

    struct ibv_qp_init_attr qp_attr;
    build_context(event->id->verbs);
    build_qp_attr(&qp_attr);

    if (rdma_create_qp(event->id, ctx->pd, &qp_attr)) error ("rdma_create_qp");

    struct conn *conn;
    event->id->context = conn = static_cast<struct conn*>(calloc(1, sizeof(*conn)));
    conn->id = event->id;
    conn->qp = event->id->qp;

    register_memory(conn);
    post_receives(conn);

    printf("Accepting connection.\n");
    if (rdma_accept(event->id, NULL)) error("rdma_accept");

    // --- julian ---

    return 0;
}

int callback_CONNECT_RESPONSE (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_CONNECT_ERROR (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_UNREACHABLE (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_REJECTED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ESTABLISHED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_DISCONNECTED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);

    // --- julian ---

    struct conn *conn = static_cast<struct conn *>(event->id->context);

    printf("Disconnecting.\n");

    rdma_destroy_qp(event->id);
    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);

    free(conn->send_region);
    free(conn->recv_region);
    free(conn);
    rdma_destroy_id(event->id);

    // --- julian ---

    return 0;
}

int callback_DEVICE_REMOVAL (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_MULTICAST_JOIN (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_MULTICAST_ERROR (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ADDR_CHANGE(struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_TIMEWAIT_EXIT (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        error ("usage: dht_server <port>");
    }

    struct rdma_event_channel * ec = nullptr;
    struct rdma_cm_id         * id = nullptr;
    struct rdma_cm_event      * ev = nullptr;

    if ((ec = rdma_create_event_channel()) == nullptr) error ("rdma_create_event_channel failed");

    if (rdma_create_id (ec, &id, nullptr, RDMA_PS_TCP) != 0) error ("rdma_create_id");
    // TODO need to call rdma_destroy_event_channel (ec) or is it done automatically at exit ?

    uint16_t port;
    char * end;
    port = strtol (argv[1], &end, 0);
    if (*end) error (-1, 0, "cannot parse port");

    if (ipv6) {
 
        struct sockaddr_in6 addr = {};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr   = in6addr_any;
        addr.sin6_port   = htons (port);

        if (rdma_bind_addr (id, reinterpret_cast<sockaddr*>(&addr))) error ("rdma_bind_addr failed");

    } else {

        struct sockaddr_in addr = {};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons (port);

        if (rdma_bind_addr (id, reinterpret_cast<sockaddr*>(&addr))) error ("rdma_bind_addr failed");

    }

    if (rdma_listen (id, LISTEN_BACKLOG)) error ("rdma_listen failed");

    uint16_t port_assigned = ntohs (rdma_get_src_port (id));

    if (port_assigned != port) error (-1,0,"requested port %hu != received port %hu\n", port, port_assigned);

    printf ("starting event loop\n");

    while (rdma_get_cm_event(ec, &ev) == 0) {

        struct rdma_cm_event copy;
        memcpy (&copy, ev, sizeof(copy));
        rdma_ack_cm_event (ev);

        printf ("rdma_get_cm_event : %u\n", copy.event);

        #define CASE(event) case RDMA_CM_EVENT_ ## event : err = callback_ ## event (&copy); \
                                                           if (err) printf ("Error:" #event "\n"); break;

        int err = 0;
        switch (copy.event) {
//            CASE(ADDR_RESOLVED);
//            CASE(ADDR_ERROR);
//            CASE(ROUTE_RESOLVED);
//            CASE(ROUTE_ERROR);
            CASE(CONNECT_REQUEST);
//            CASE(CONNECT_RESPONSE);
//            CASE(CONNECT_ERROR);
//            CASE(UNREACHABLE);
//            CASE(REJECTED);
            CASE(ESTABLISHED);
            CASE(DISCONNECTED);
//            CASE(DEVICE_REMOVAL);
//            CASE(MULTICAST_JOIN);
//            CASE(MULTICAST_ERROR);
//            CASE(ADDR_CHANGE);
//            CASE(TIMEWAIT_EXIT);
            default:
                err = 1;
                printf ("unhandled event type %d, treating as error\n", copy.event);
        }

        if (err) {
            printf ("errno = %d\n", errno);
            perror ("switch");
            break;
        }
    }

    usleep(100000);   // 100 ms

    rdma_destroy_id(id);
    rdma_destroy_event_channel(ec);

    return 0;
}
