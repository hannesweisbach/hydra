#include <iomanip>

#include "hydra/network.h"

namespace hydra {
namespace overlay {
std::ostream &operator<<(std::ostream &s, const node_id &id) {
  return s << id.ip << ":" << id.port << " " << std::setw(6) << hex(id.id);
}

std::ostream &operator<<(std::ostream &s, const routing_entry &e) {
  return s << e.node << " " << std::setw(6) << e.start << " [" << e.node.id - e.start << "]";
}

void init_node(const std::string &host, const std::string &port,
               hydra::protocol::Node::Builder &n) {
  assert(host.size() < 16);
  assert(port.size() < 6);
  auto host_ = n.initIp(static_cast<uint32_t>(host.size()));
  auto port_ = n.initPort(static_cast<uint32_t>(port.size()));

  host.copy(std::begin(host_), host.size());
  port.copy(std::begin(port_), port.size());
}

routing_table::routing_table(const std::string &host, const std::string &port)
    : local_host(host), local_port(port) {}

kj::Array<capnp::word> routing_table::process_message(
    const hydra::protocol::DHTRequest::Overlay::Reader &overlay) {
  switch (overlay.which()) {
  case hydra::protocol::DHTRequest::Overlay::NETWORK:
    return init();
  case hydra::protocol::DHTRequest::Overlay::JOIN: {
    auto node = overlay.getJoin().getNode();
    return process_join(node.getIp().cStr(), node.getPort().cStr());
  }
  case hydra::protocol::DHTRequest::Overlay::UPDATE: {
    auto update_ = overlay.getUpdate();
    auto node = update_.getNode();
    keyspace_t id;
    auto id_ = update_.getId();
    assert(id_.size() == sizeof(id));
    memcpy(&id, std::begin(id_), sizeof(id));
    update(node.getIp().cStr(), node.getPort().cStr(), id, update_.getIndex());
  } break;
  default:
    break;
  }
  return kj::Array<capnp::word>();
}

kj::Array<capnp::word> join_request(const std::string &host,
                                    const std::string &port) {

  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTRequest>();

  auto node = msg.initOverlay().initJoin().initNode();
  init_node(host, port, node);
  return messageToFlatArray(message);
}

kj::Array<capnp::word> join_reply(const keyspace_t &start,
                                  const keyspace_t &end, const bool) {
  ::capnp::MallocMessageBuilder response;
  hydra::protocol::DHTResponse::Builder msg =
      response.initRoot<hydra::protocol::DHTResponse>();

  auto join = msg.initJoin();
  auto start_ = join.initStart(sizeof(start));
  auto end_ = join.initEnd(sizeof(end));
  memcpy(std::begin(start_), &start, sizeof(start));
  memcpy(std::begin(end_), &end, sizeof(end));
  return messageToFlatArray(response);
}

kj::Array<capnp::word> update_message(const std::string &host,
                                      const std::string &port,
                                      const keyspace_t &id,
                                      const size_t index) {
  ::capnp::MallocMessageBuilder response;
  auto msg = response.initRoot<hydra::protocol::DHTRequest>();

  auto update = msg.initOverlay().initUpdate();
  update.setIndex(index);
  auto node = update.initNode();
  init_node(host, port, node);
  auto id_ = update.initId(sizeof(id));
  memcpy(std::begin(id_), &id, sizeof(id));

  return messageToFlatArray(response);
}

kj::Array<capnp::word> network_request() {
  ::capnp::MallocMessageBuilder request;
  request.initRoot<hydra::protocol::DHTRequest>().initOverlay().setNetwork();
  return messageToFlatArray(request);
}


}
}

