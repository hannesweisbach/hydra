#pragma once

#include <limits>
#include <iterator>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"
#include "rdma/addressof.h"
#include "rdma/RDMAWrapper.hpp"

namespace hydra {
namespace rdma {
template <> const void *address_of(const kj::Array<capnp::word> &o);
template <> size_t size_of(const kj::Array<capnp::word> &o);
}
}

kj::Array<capnp::word> init_message();
kj::Array<capnp::word> ack_message(const bool);

  template <typename T>
kj::Array<capnp::word>
put_message(const rdma_ptr<T> &key, const size_t &key_size,
            const rdma_ptr<T> &value, const size_t &value_size) {
  assert(key_size <= std::numeric_limits<uint32_t>::max());
  assert(value_size <= std::numeric_limits<uint32_t>::max());
  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto remote = msg.initPut().initRemote();
  auto key_mr = remote.initKey();
  key_mr.setAddr(reinterpret_cast<uint64_t>(key.first.get()));
  key_mr.setSize(static_cast<uint32_t>(key_size));
  key_mr.setRkey(key.second->rkey);
  auto value_mr = remote.initValue();
  value_mr.setAddr(reinterpret_cast<uint64_t>(value.first.get()));
  value_mr.setSize(static_cast<uint32_t>(value_size));
  value_mr.setRkey(value.second->rkey);

  return messageToFlatArray(message);
}

template <typename T>
kj::Array<capnp::word> put_message_inline(const T &o, const size_t &key_size) {
  using namespace hydra::rdma;
  const size_t size = size_of(o);
  const void *ptr = address_of(o);

  assert(size <= std::numeric_limits<uint8_t>::max());

  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto put = msg.initPut().initInline();

  put.setKeySize(static_cast<uint8_t>(key_size));
  put.setSize(static_cast<uint8_t>(size));
  auto key_data = put.initData(static_cast<uint8_t>(size));
  memcpy(std::begin(key_data), ptr, size);

  return messageToFlatArray(message);
}

