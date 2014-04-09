#include "hydra/chord.h"
#if 0
#include "hydra/passive.h"
#else
#include "hydra/types.h"
#include "hydra/messages.h"
#include "rdma/RDMAClientSocket.h"
#endif

namespace hydra {
namespace chord {
static routing_table find_table(routing_table table, const keyspace_t &id) {
  while (!id.in(table.self().node.id + 1, table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
#if 0
    hydra::passive node(re.ip, re.port);
#else
    /* Ok. this appears also in client.cc. Wrap. */
    RDMAClientSocket socket(re.ip, re.port);
    socket.connect();
    auto init = socket.recv_async<mr_response>();
    init_request request;
    socket.sendImmediate(request);

    init.first.get(); // block.
    auto mr = init.second.first->value();
    auto info = socket.read<hydra::node_info>(mr.addr, mr.rkey);
    info.first.get(); // block

    auto table_ = socket.read<RDMAObj<routing_table> >(
        reinterpret_cast<uintptr_t>(info.second.first->routing_table.addr),
        info.second.first->routing_table.rkey);
    table_.first.get();
    table = table_.second.first->get();
#endif
    //table = node.table();
  }
  return table;
}

node_id predecessor(const routing_table &start, const keyspace_t &id) {
  return find_table(start, id).self().node;
}

node_id successor(const routing_table &start, const keyspace_t &id) {
  return find_table(start, id).successor().node;
}
}
}
