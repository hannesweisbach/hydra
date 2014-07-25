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

kj::Array<capnp::word> network_request() {
  ::capnp::MallocMessageBuilder request;
  request.initRoot<hydra::protocol::DHTRequest>().setNetwork();
  return messageToFlatArray(request);
}


