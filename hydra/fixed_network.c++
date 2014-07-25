#include <exception>

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
}
}
}

