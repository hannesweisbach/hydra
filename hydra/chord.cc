#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/client.h"
#include "hydra/protocol/message.h"

#include "hydra/RDMAObj.h"

namespace hydra {
namespace overlay {
chord::chord(const std::string &host, const std::string &port)
    : RDMAClientSocket(host, port),
      table(std::make_unique<RDMAObj<hydra::routing_table> >()),
      local_table_mr(register_memory(ibv_access::MSG, *table)) {
  connect();

  kj::FixedArray<capnp::word, 9> response;
  auto mr = register_memory(ibv_access::MSG, response);

  auto future = recv_async(response, mr.get());

  send(network_request());
  future.get();

  auto message = capnp::FlatArrayMessageReader(response);
  auto reader = message.getRoot<protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::NETWORK);
  auto network = reader.getNetwork();
  assert(network.getType() == hydra::protocol::DHTResponse::NetworkType::CHORD);
  auto t = network.getTable();
  table_mr.addr = t.getAddr();
  table_mr.size = t.getSize();
  table_mr.rkey = t.getRkey();
}

chord::chord(const std::string &host, const std::string &port,
           const uint64_t addr, const size_t size, const uint32_t rkey)
    : RDMAClientSocket(host, port),
      table(std::make_unique<RDMAObj<hydra::routing_table> >()),
      local_table_mr(register_memory(ibv_access::MSG, *table)) {
  table_mr.addr = addr;
  table_mr.size = size;
  table_mr.rkey = rkey;
}

chord::~chord() {}

hydra::routing_table chord::load_table() const {
  hydra::rdma::load(*this, *table, local_table_mr.get(), table_mr.addr,
                    table_mr.rkey);
  return (*table).get();
}

hydra::routing_table chord::find_table(const keyspace_t &id) const {
  using namespace hydra::literals;

  auto table = load_table();
  while (!id.in(table.self().node.id + 1_ID, table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    chord node(re.ip, re.port);

    table = node.load_table();
  }
  return table;
}

node_id chord::predecessor_node(const keyspace_t &id) const {
  return find_table(id).self().node;
}

node_id chord::successor_node(const keyspace_t &id) const {
  return find_table(id).successor().node;
}

node_id chord::self() const {
  auto table = load_table();
  return table.self().node;
}

passive &chord::successor(const keyspace_t &id) {
  auto table = find_table(id);
  auto start = table.predecessor().node.id + 1_ID;
  auto end = table.self().node.id;
  if (cache.empty()) {
    cache.emplace_back(start, end, table.self().node.ip,
                       table.self().node.port);
  } else {
    cache[0] =
        network::node(start, end, table.self().node.ip, table.self().node.port);
  }
  return cache[0];
}
}
}

