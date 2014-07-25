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

static kj::Array<capnp::word>
network_response(const rdma_ptr<LocalRDMAObj<routing_table> > &table,
                 hydra::protocol::DHTResponse::NetworkType type,
                 const uint16_t size = 0) {
  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTResponse>();

  auto network = msg.initNetwork();
  network.setType(type);
  network.setSize(size);
  auto remote = network.initTable();
  remote.setAddr(reinterpret_cast<uintptr_t>(table.first.get()));
  remote.setSize(sizeof(LocalRDMAObj<routing_table>));
  remote.setRkey(table.second->rkey);

  return messageToFlatArray(message);
}

kj::Array<capnp::word>
chord_response(const rdma_ptr<LocalRDMAObj<routing_table> > &table) {
  return network_response(table,
                          hydra::protocol::DHTResponse::NetworkType::CHORD);
}

kj::Array<capnp::word>
fixed_response(const rdma_ptr<LocalRDMAObj<routing_table> > &table,
               const uint16_t size) {
  return network_response(
      table, hydra::protocol::DHTResponse::NetworkType::FIXED, size);
}
}
}
}

