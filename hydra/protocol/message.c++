#include "message.h"

namespace hydra {
namespace rdma {
template <> const void *address_of(const kj::Array<capnp::word> &o) {
  return o.begin();
}

template <> size_t size_of(const kj::Array<capnp::word> &o) {
  return o.size() * sizeof(capnp::word);
}
}
}

kj::Array<capnp::word> init_message() {
  ::capnp::MallocMessageBuilder request;
  request.initRoot<hydra::protocol::DHTRequest>().setInit();
  return messageToFlatArray(request);
}

kj::Array<capnp::word> ack_message(const bool success) {
  ::capnp::MallocMessageBuilder response;
  hydra::protocol::DHTResponse::Builder msg =
      response.initRoot<hydra::protocol::DHTResponse>();

  msg.initAck().setSuccess(success);
  return messageToFlatArray(response);
}

static void init_node(const hydra::node_id &node,
                      hydra::protocol::Node::Builder &n) {
  auto ip = n.initIp(sizeof(node.ip));
  auto port = n.initPort(sizeof(node.port));
  auto id = n.initId(sizeof(node.id));

  memcpy(std::begin(ip), node.ip, sizeof(node.ip));
  memcpy(std::begin(port), node.port, sizeof(node.port));
  memcpy(std::begin(id), &node.id, sizeof(node.id));
}

kj::Array<capnp::word> predecessor_message(const hydra::node_id &node) {
  ::capnp::MallocMessageBuilder response;

  auto n = response.initRoot<hydra::protocol::DHTRequest>()
               .initPredecessor()
               .initNode();
  init_node(node, n);
  return messageToFlatArray(response);
}

kj::Array<capnp::word> update_message(const hydra::node_id &node,
                                      const size_t &index) {
  ::capnp::MallocMessageBuilder response;

  auto update = response.initRoot<hydra::protocol::DHTRequest>().initUpdate();
  update.setIndex(index);
  auto n = update.initNode();
  init_node(node, n);
  return messageToFlatArray(response);

}

kj::Array<capnp::word> chord_request() {
  ::capnp::MallocMessageBuilder request;
  request.initRoot<hydra::protocol::DHTRequest>().setChord();
  return messageToFlatArray(request);
}

kj::Array<capnp::word>
chord_response(const rdma_ptr<LocalRDMAObj<hydra::routing_table> > &table) {
  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTResponse>();

  auto remote = msg.initChord().initTable();
  remote.setAddr(reinterpret_cast<uintptr_t>(table.first.get()));
  remote.setSize(sizeof(LocalRDMAObj<hydra::routing_table>));
  remote.setRkey(table.second->rkey);

  return messageToFlatArray(message);
}

