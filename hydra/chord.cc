#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/client.h"
#include "hydra/protocol/message.h"

#include "hydra/RDMAObj.h"
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"

namespace hydra {
namespace overlay {
namespace chord {

chord::chord(const std::string &host, const std::string &port)
    : RDMAClientSocket(host, port),
      table(std::make_unique<RDMAObj<routing_table> >()),
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
      table(std::make_unique<RDMAObj<routing_table> >()),
      local_table_mr(register_memory(ibv_access::MSG, *table)) {
  table_mr.addr = addr;
  table_mr.size = size;
  table_mr.rkey = rkey;
}

chord::~chord() {}

routing_table chord::load_table() const {
  hydra::rdma::load(*this, *table, local_table_mr.get(), table_mr.addr,
                    table_mr.rkey);
  return (*table).get();
}

routing_table chord::find_table(const keyspace_t &id) const {
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

static void init_node(const node_id &node, hydra::protocol::Node::Builder &n) {
  auto ip = n.initIp(sizeof(node.ip));
  auto port = n.initPort(sizeof(node.port));
  auto id = n.initId(sizeof(node.id));

  memcpy(std::begin(ip), node.ip, sizeof(node.ip));
  memcpy(std::begin(port), node.port, sizeof(node.port));
  memcpy(std::begin(id), &node.id, sizeof(node.id));
}

kj::Array<capnp::word> predecessor_message(const node_id &node) {
  ::capnp::MallocMessageBuilder response;

  auto n = response.initRoot<hydra::protocol::DHTRequest>()
               .initPredecessor()
               .initNode();
  init_node(node, n);
  return messageToFlatArray(response);
}

kj::Array<capnp::word> update_message(const node_id &node,
                                      const size_t &index) {
  ::capnp::MallocMessageBuilder response;

  auto update = response.initRoot<hydra::protocol::DHTRequest>().initUpdate();
  update.setIndex(index);
  auto n = update.initNode();
  init_node(node, n);
  return messageToFlatArray(response);

}

std::ostream &operator<<(std::ostream &s, const routing_table &t) {
  s << "routing_table " << t.table.size() << std::endl;
  {
    indent_guard guard(s);
    s << indent << "pred: " << t.predecessor() << std::endl;
    s << indent << "self: " << t.self() << std::endl;
    size_t i = 1;
    for (auto &&e : t) {
      s << indent << "[" << std::setw(3) << i++ << "] " << e
        << " " << std::endl;
    }
  }
  return s;
}

}
}
}

