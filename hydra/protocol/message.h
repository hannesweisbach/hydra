#pragma once

#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"
#include "rdma/addressof.h"

namespace hydra {
namespace rdma {
template <> const void *address_of(const kj::Array<capnp::word> &o);
template <> size_t size_of(const kj::Array<capnp::word> &o);
}
}

