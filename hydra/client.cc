#include "hydra/client.h"
#include "hydra/passive.h"
#include "hydra/hash.h"

#include "util/Logger.h"

#include "hydra/protocol/message.h"

namespace hydra {
hydra::node_info get_info(const RDMAClientSocket &socket) {
  auto init = socket.recv_async<kj::FixedArray<capnp::word, 9> >();

  socket.send(init_message());

  init.first.get(); // block.

  auto reply = capnp::FlatArrayMessageReader(*init.second.first);
  auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

  assert(reader.which() == hydra::protocol::DHTResponse::INIT);

  auto mr = reader.getInit().getInfo();
  assert(mr.getSize() >= sizeof(hydra::node_info));
  auto info = socket.read<hydra::node_info>(mr.getAddr(), mr.getRkey());
  info.first.get(); // block

  return *info.second.first;
}
}

hydra::client::client(const std::string &ip, const std::string &port)
    : root(ip, port) {}

hydra::node_id
hydra::client::responsible_node(const std::vector<unsigned char> &key) const {
  return root.successor(keyspace_t(hydra::hash(key)));
}

hydra::node_info hydra::client::get_info(const RDMAClientSocket &socket) const {
  return ::hydra::get_info(socket);
}

bool hydra::client::add(const std::vector<unsigned char> &key,
                        const std::vector<unsigned char> &value) const {
  auto start = std::chrono::high_resolution_clock::now();
  const auto nodeid = responsible_node(key);
  auto end = std::chrono::high_resolution_clock::now();
  // log_info() << "Blocked for "
  //           << std::chrono::duration_cast<std::chrono::microseconds>(
  //                  end - start).count();
  const hydra::passive dht(nodeid.ip, nodeid.port);
  std::vector<unsigned char> kv(key);
  kv.insert(std::end(kv), std::begin(value), std::end(value));
  return dht.put(kv, key.size());
}

bool hydra::client::remove(const std::vector<unsigned char> &key) const {
  const auto nodeid = responsible_node(key);
  const hydra::passive dht(nodeid.ip, nodeid.port);
  return dht.remove(key);
}

bool hydra::client::contains(const std::vector<unsigned char> &key) const {
  const auto nodeid = responsible_node(key);
  const hydra::passive dht(nodeid.ip, nodeid.port);
  return dht.contains(key);
}

std::vector<unsigned char>
hydra::client::get(const std::vector<unsigned char> &key) const {
  const auto nodeid = responsible_node(key);
  const hydra::passive dht(nodeid.ip, nodeid.port);
  return dht.get(key);
}

