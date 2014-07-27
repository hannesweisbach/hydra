#include <exception>
#include <iterator>
#include <algorithm>
#include <sstream>

#include "hydra/fixed_network.h"
#include "hydra/passive.h"
#include "dht.capnp.h"

namespace hydra {
namespace overlay {
namespace fixed {
fixed::fixed(RDMAClientSocket &root, uint64_t addr, const uint32_t rkey,
             const uint16_t entries) {
  entry_t entry;
  auto mr = root.register_memory(ibv_access::READ, entry);

  std::generate_n(std::back_inserter(nodes), entries, [&]() {
    hydra::rdma::load(root, entry, mr.get(), addr, rkey);
    addr += sizeof(entry);

    const auto &e = entry.get();
    if (e.empty()) {
      throw std::runtime_error("Empty entry in routing table would require "
                               "dynamic join. But dynamic joining is not "
                               "supported.");
    }
    return network::node(e.start, e.node.id, e.node.ip, e.node.port);
  });
}

passive &fixed::successor(const keyspace_t &id) {
  auto contains = std::bind(&node::contains, std::placeholders::_1, id);
  auto result = std::find_if(std::begin(nodes), std::end(nodes), contains);
  if (result == std::end(nodes))
    throw std::runtime_error("Host not found");

  return *result;
}

routing_table::routing_table(RDMAServerSocket &socket, const std::string &host,
                             const std::string &port, uint16_t size)
    : hydra::overlay::routing_table(host, port) {
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
  auto result =
      std::find_if(std::begin(table), std::end(table),
                   [](const auto &entry) { return entry.get().empty(); });

  if (result == std::end(table)) {
    //TODO add success indicator to reply.
    throw std::runtime_error(
        "Tried joining a full network. Error handling not implemented.");
  }

  (*result)([&](auto &&entry) {
    assert(host.size() < sizeof(node_id::ip));
    assert(port.size() < sizeof(node_id::port));

    host.copy(entry.node.ip, sizeof(node_id::ip));
    port.copy(entry.node.port, sizeof(node_id::port));
  });

  auto msg = update_message(host, port, result->get().node.id,
                            std::distance(std::begin(table), result));
  for (const auto &entry : table) {
    // TODO may avoid sending an update message to this node.
    const auto &node = entry.get();
    if (!node.empty()) {
      RDMAClientSocket s(node.node.ip, node.node.port);
      s.connect();
      s.send(msg);
    }
  }

  return join_reply(result->get().start, result->get().node.id);
}

void routing_table::update(const std::string &host, const std::string &port,
                           const keyspace_t &id, const size_t index) {
  if (index < table.size()) {
    const auto &it = std::begin(table) + index;
    (*it)([&](auto &&entry) {
      assert(host.size() < sizeof(node_id::ip));
      assert(port.size() < sizeof(node_id::port));
      assert(id == entry.node.id);

      host.copy(entry.node.ip, sizeof(node_id::ip));
      port.copy(entry.node.port, sizeof(node_id::port));
    });
  } else {
    std::ostringstream ss;
    ss << "Request to update non-existent entry " << index
       << " in table of size " << table.size();
    throw std::runtime_error(ss.str());
  }
}
}
}
}

