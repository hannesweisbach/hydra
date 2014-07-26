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
  auto host_ = n.initIp(host.size());
  auto port_ = n.initPort(port.size());

  host.copy(std::begin(host_), host.size());
  port.copy(std::begin(port_), port.size());
}

kj::Array<capnp::word> join_message(const std::string &host,
                                    const std::string &port) {

  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTRequest>();

  auto node = msg.initJoin().initNode();
  init_node(host, port, node);
  return messageToFlatArray(message);
}
}
}

