#include <exception>
#include <iterator>
#include <algorithm>

#include "hydra/fixed_network.h"
#include "hydra/passive.h"
#include "dht.capnp.h"

namespace hydra {
namespace overlay {
namespace fixed {
fixed::fixed(RDMAClientSocket &root, const uint64_t addr, const size_t size,
             const uint32_t rkey) {
  // check fixed layout
  // read table
  // init vector
}

passive &fixed::successor(const keyspace_t &id) {
  auto contains = std::bind(&node::contains, std::placeholders::_1, id);
  auto result = std::find_if(std::begin(nodes), std::end(nodes), contains);
  if (result == std::end(nodes))
    throw std::runtime_error("Host not found");

  return *result;
}

routing_table::routing_table(RDMAServerSocket &socket, uint16_t size) {
  if (size == 0)
    throw std::runtime_error("Routing table of size 0 is not supported.");

  auto keys = keyspace_t(std::numeric_limits<keyspace_t::value_type>::max());
  auto id = 0_ID;
  auto generator = [&]() mutable {
    assert(size);
    auto partition = keys / keyspace_t(size);
    keys -= partition;
    size--;
    auto id_ = id;
    id += partition;
    return entry_t("", "", id_, id - 1_ID);
  };

  std::generate_n(std::back_inserter(table), size, generator);
  table_mr = socket.register_memory(ibv_access::READ, table);

  for (const auto &e : table)
    std::cout << e.get() << std::endl;
}

kj::Array<capnp::word> routing_table::init() const {
  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTResponse>();

  auto network = msg.initNetwork();
  network.setType(hydra::protocol::DHTResponse::NetworkType::FIXED);
  network.setSize(static_cast<uint16_t>(table.size()));
  auto remote = network.initTable();
  remote.setAddr(reinterpret_cast<uintptr_t>(table_mr->addr));
  remote.setSize(table_mr->length);
  remote.setRkey(table_mr->rkey);
  return messageToFlatArray(message);
}

kj::Array<capnp::word> routing_table::join(const std::string &host,
                                           const std::string &port) {
  return kj::Array<capnp::word>();
}

}
}
}

