#include <stdio.h>
#include <error.h>
#include <errno.h>
#include <netdb.h>  // getaddrinfo
#include <string.h>

#include <rdma/rdma_cma.h>

int const ADDR_RESOLVE_TIMEOUT = 1000;      // milli seconds
int const ROUTE_RESOLVE_TIMEOUT = 1000;     // milli seconds

int callback_ADDR_RESOLVED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return rdma_resolve_route (event->id, ROUTE_RESOLVE_TIMEOUT);
}

int callback_ADDR_ERROR (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
    return 0;
}

int callback_ROUTE_RESOLVED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);

    struct ibv_pd * pd = ibv_alloc_pd (event->id->verbs);
    if (not pd) perror ("ibv_alloc_pd failed");

    struct ibv_comp_channel * chan = ibv_create_comp_channel (event->id->verbs);
    if (not chan) perror ("ibv_create_comp_channel failed");

    // one outstanding work request, no context yet, completion channel, no msi-x vector
    struct ibv_cq * cq = ibv_create_cq (event->id->verbs, 1, nullptr, chan, 0);
    if (not cq) perror ("ibv_create_cq failed");

    if (ibv_req_notify_cq (cq, 0)) perror ("ibv_req_notify_cq failed");

    struct ibv_qp_init_attr qp_attr = {};

    // same completion queue for send and receive events
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;

    // Queue Pair Transport Service Type: Reliable Connection, Unreliable Connection, Unreliable Datagram
    qp_attr.qp_type = IBV_QPT_RC;   // reliable conn

    qp_attr.cap.max_send_wr  = 1;   // max outstanding send requests
    qp_attr.cap.max_send_sge = 1;   // max send scatter gather element
    qp_attr.cap.max_recv_wr  = 1;   // max outsranding receive requests
    qp_attr.cap.max_recv_sge = 1;   // max recv scatter gather element

    if (rdma_create_qp (event->id, pd, &qp_attr)) perror ("rdma_create_qp failed");

    struct rdma_conn_param conn_param = {};

    conn_param.initiator_depth = 1;
    conn_param.retry_count = 1;

    if (rdma_connect (event->id, &conn_param)) perror ("rdma_connect failed");

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

    // TODO : continue here

    return 0;
}

int callback_DISCONNECTED (struct rdma_cm_event * event)
{
    printf ("%s called\n", __FUNCTION__);
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
    if (argc != 3) {
        error (-1, 0, "usage: dht_client <host> <port>");
    }

    struct addrinfo * addr = nullptr;

    struct rdma_event_channel * ec  = nullptr;
    struct rdma_cm_id         * id = nullptr;
    struct rdma_cm_event      * ev = nullptr;

    if ((ec = rdma_create_event_channel()) == nullptr) perror ("rdma_create_event_channel failed");

    if (rdma_create_id (ec, &id, nullptr, RDMA_PS_TCP) != 0) perror ("rdma_create_id");
    // TODO need to call rdma_destroy_event_channel (ec) or is it done automatically at exit ?

    if (getaddrinfo (argv[1], argv[2], nullptr, &addr) != 0) perror ("getaddrinfo failed");

    struct addrinfo * it = addr;
    for (; it; it = it->ai_next) {
        if (rdma_resolve_addr (id, nullptr, it->ai_addr, ADDR_RESOLVE_TIMEOUT) == 0) {
            break;
        }
    }
    if (!it) perror ("rdma_resolve_addr failed");
    freeaddrinfo(addr);

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
            CASE(ADDR_RESOLVED);    // rdma_resolve_addr
            CASE(ADDR_ERROR);       // rdma_resolve_addr
            CASE(ROUTE_RESOLVED);   // rdma_resolve_route
            CASE(ROUTE_ERROR);      // rdma_resolve_route
            CASE(CONNECT_REQUEST);
            CASE(CONNECT_RESPONSE);
            CASE(CONNECT_ERROR);
            CASE(UNREACHABLE);
            CASE(REJECTED);
            CASE(ESTABLISHED);
            CASE(DISCONNECTED);
            CASE(DEVICE_REMOVAL);
            CASE(MULTICAST_JOIN);
            CASE(MULTICAST_ERROR);
            CASE(ADDR_CHANGE);
            CASE(TIMEWAIT_EXIT);
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

    // rdma_destroy_id(id);
    // rdma_destroy_event_channel(ec);

    return 0;
}
