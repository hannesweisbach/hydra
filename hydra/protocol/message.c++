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



