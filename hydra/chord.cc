#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/client.h"

namespace hydra {
namespace chord {
static routing_table find_table(routing_table table, const keyspace_t &id) {
  while (
      !id.in(table.self().node.id + keyspace_t(1), table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    /* Ok. this appears also in client.cc. Wrap. */
    RDMAClientSocket socket(re.ip, re.port);
    socket.connect();

    const hydra::node_info info = hydra::get_info(socket);

    auto table_ = socket.read<RDMAObj<routing_table> >(
        reinterpret_cast<uintptr_t>(info.routing_table.addr),
        info.routing_table.rkey);
    table_.first.get();
    table = table_.second.first->get();
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
