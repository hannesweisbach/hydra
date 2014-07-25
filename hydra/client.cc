#include "hydra/client.h"
#include "hydra/passive.h"
#include "hydra/hash.h"

#include "hydra/fixed_network.h"
#include "hydra/chord.h"

#include "util/Logger.h"

#include "hydra/protocol/message.h"

hydra::client::client(const std::string &ip, const std::string &port) {
  RDMAClientSocket node(ip, port);
  node.connect();

  kj::FixedArray<capnp::word, 9> response;
  auto response_mr = node.register_memory(ibv_access::MSG, response);

  auto future = node.recv_async(response, response_mr.get());

  node.send(network_request());
  future.get();

  auto message = capnp::FlatArrayMessageReader(response);
  auto reader = message.getRoot<protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::NETWORK);
  auto network_msg = reader.getNetwork();
  auto t = network_msg.getTable();
  switch (network_msg.getType()) {
  case hydra::protocol::DHTResponse::NetworkType::FIXED:
    network = std::make_unique<hydra::overlay::fixed::fixed>(
        node, t.getAddr(), t.getSize(), t.getRkey());
    break;
  case hydra::protocol::DHTResponse::NetworkType::CHORD:
    network = std::make_unique<hydra::overlay::chord>(ip, port, t.getAddr(),
                                                      t.getSize(), t.getRkey());
    break;
  }
}

hydra::passive &
hydra::client::responsible_node(const std::vector<unsigned char> &key) const {
  return network->successor(keyspace_t(hydra::hash(key)));
}

bool hydra::client::add(const std::vector<unsigned char> &key,
                        const std::vector<unsigned char> &value) const {
  auto start = std::chrono::high_resolution_clock::now();
  auto &&dht = responsible_node(key);
  auto end = std::chrono::high_resolution_clock::now();
  // log_info() << "Blocked for "
  //           << std::chrono::duration_cast<std::chrono::microseconds>(
  //                  end - start).count();
  std::vector<unsigned char> kv(key);
  kv.insert(std::end(kv), std::begin(value), std::end(value));
  return dht.put(kv, key.size());
}

bool hydra::client::remove(const std::vector<unsigned char> &key) const {
  auto &&dht = responsible_node(key);
  return dht.remove(key);
}

bool hydra::client::contains(const std::vector<unsigned char> &key) const {
  auto &&dht = responsible_node(key);
  return dht.contains(key);
}

std::vector<unsigned char>
hydra::client::get(const std::vector<unsigned char> &key) const {
  auto &&dht = responsible_node(key);
  return dht.get(key);
}

