#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/client.h"
#include "hydra/protocol/message.h"

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

node::node(const std::string &host, const std::string &port)
    : RDMAClientSocket(host, port) {
  connect();

  auto response = recv_async<kj::FixedArray<capnp::word, 9> >();

  send(chord_request());
  response.first.get();

  auto message = capnp::FlatArrayMessageReader(*response.second.first);
  auto reader = message.getRoot<protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::CHORD);
  auto t = reader.getChord().getTable();
  table_mr.addr = t.getAddr();
  table_mr.size = t.getSize();
  table_mr.rkey = t.getRkey();
}

hydra::routing_table node::load_table() const {
  auto table =
      read<RDMAObj<hydra::routing_table> >(table_mr.addr, table_mr.rkey);
  table.first.get();
  return table.second.first->get();
}

hydra::routing_table node::find_table(const keyspace_t &id) const {
  using namespace hydra::literals;

  auto table = load_table();
  while (!id.in(table.self().node.id + 1_ID, table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    node node(re.ip, re.port);

    table = node.load_table();
  }
  return table;
}

node_id node::predecessor(const keyspace_t &id) const {
  return find_table(id).self().node;
}

node_id node::successor(const keyspace_t &id) const {
  return find_table(id).successor().node;
}

node_id node::self() const {
  auto table = load_table();
  return table.self().node;
}
}
}

