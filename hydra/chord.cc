#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/protocol/message.h"
#include "rdma/RDMAClientSocket.h"

namespace hydra {
namespace chord {
static routing_table find_table(routing_table table, const keyspace_t &id) {
  while (
      !id.in(table.self().node.id + keyspace_t(1), table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    /* Ok. this appears also in client.cc. Wrap. */
    RDMAClientSocket socket(re.ip, re.port);
    socket.connect();

    auto init = socket.recv_async<kj::FixedArray<capnp::word, 9> >();
    socket.sendImmediate(init_message());

    init.first.get(); // block.

    auto reply = capnp::FlatArrayMessageReader(*init.second.first);
    auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

    assert(reader.which() == hydra::protocol::DHTResponse::INIT);

    auto mr = reader.getInit().getInfo();
    assert(mr.getSize() >= sizeof(hydra::node_info));
    auto info = socket.read<hydra::node_info>(mr.getAddr(), mr.getRkey());
    info.first.get(); // block

    auto table_ = socket.read<RDMAObj<routing_table> >(
        reinterpret_cast<uintptr_t>(info.second.first->routing_table.addr),
        info.second.first->routing_table.rkey);
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
