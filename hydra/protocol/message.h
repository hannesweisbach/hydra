#pragma once

#include <limits>
#include <iterator>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"
#include "rdma/addressof.h"
#include "rdma/RDMAWrapper.hpp"
#include "hydra/types.h" //node_id

namespace hydra {
namespace rdma {
template <> const void *address_of(const kj::Array<capnp::word> &o);
template <> size_t size_of(const kj::Array<capnp::word> &o);
}
}

kj::Array<capnp::word> init_message();
kj::Array<capnp::word> ack_message(const bool);
kj::Array<capnp::word> network_request();

template <typename T>
kj::Array<capnp::word> put_message(const T &kv, const size_t &key_size,
                                   const uint32_t rkey) {
  using namespace hydra::rdma;
  const size_t size = size_of(kv);
  const void *ptr = address_of(kv);
  assert(key_size <= std::numeric_limits<uint32_t>::max());
  assert(size <= std::numeric_limits<uint32_t>::max());
  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto remote = msg.initPut().initRemote();
  auto kv_mr = remote.initKv();
  kv_mr.setAddr(reinterpret_cast<uint64_t>(ptr));
  kv_mr.setSize(static_cast<uint32_t>(size));
  kv_mr.setRkey(rkey);
  remote.setKeySize(static_cast<uint32_t>(key_size));

  return messageToFlatArray(message);
}

template <typename T>
kj::Array<capnp::word> put_message(const rdma_ptr<T> &kv, const size_t &size,
                                   const size_t &key_size) {
  assert(key_size <= std::numeric_limits<uint32_t>::max());
  assert(size <= std::numeric_limits<uint32_t>::max());
  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto remote = msg.initPut().initRemote();
  auto kv_mr = remote.initKv();
  kv_mr.setAddr(reinterpret_cast<uint64_t>(kv.first.get()));
  kv_mr.setSize(static_cast<uint32_t>(size));
  kv_mr.setRkey(kv.second->rkey);
  remote.setKeySize(static_cast<uint32_t>(key_size));

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

template <typename T>
kj::Array<capnp::word> del_message(const rdma_ptr<T> &key,
                                   const size_t &key_size) {
  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto remote = msg.initDel().initRemote();
  auto key_mr = remote.initKey();
  key_mr.setAddr(reinterpret_cast<uint64_t>(key.first.get()));
  key_mr.setSize(static_cast<uint32_t>(key_size));
  key_mr.setRkey(key.second->rkey);

  return messageToFlatArray(message);
}

template <typename T> kj::Array<capnp::word> del_message_inline(const T &key) {
  using namespace hydra::rdma;
  const size_t size = size_of(key);
  const void *ptr = address_of(key);

  assert(size <= std::numeric_limits<uint8_t>::max());

  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto remote = msg.initDel().initInline();
  remote.setSize(static_cast<uint8_t>(size));
  auto key_data = remote.initKey(static_cast<uint8_t>(size));
  memcpy(std::begin(key_data), ptr, size);

  return messageToFlatArray(message);
}
