#include <iostream>
#include <algorithm>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"

int main() {
  ::capnp::MallocMessageBuilder mbuilder;
  hydra::protocol::DHTRequest::Builder msg =
      mbuilder.initRoot<hydra::protocol::DHTRequest>();

  auto put = msg.initPut();
  auto remote = put.initRemote();
  auto kv = remote.initKv();
  kv.setAddr(0xdeadbabe);
  kv.setSize(0x900d515e);
  kv.setRkey(0xbeef);
  remote.setKeySize(0x900d515f);

  auto send = messageToFlatArray(mbuilder);

  kj::FixedArray<capnp::word, 9> req;
  size_t copy_size = std::min(send.size(), req.size());
  memcpy(req.begin(), send.begin(), copy_size * sizeof(capnp::word));

  auto reply = capnp::FlatArrayMessageReader(req);
  auto reader = reply.getRoot<hydra::protocol::DHTRequest>();

  std::cout << (int)reader.which() << std::endl;

  if (reader.which() == hydra::protocol::DHTRequest::PUT) {
    auto put = reader.getPut();
    if (put.isRemote()) {
      auto remote = put.getRemote();
      std::cout << std::hex;
      std::cout << "remote" << std::endl;
      auto kv_reader = remote.getKv();
      std::cout << "kvr: " << kv_reader.getAddr() << " " << kv_reader.getSize()
                << " " << kv_reader.getRkey() << std::endl;
    } else {
      std::cout << "inline" << std::endl;
    }
  } else {
    std::cout << "unhandled message type " << static_cast<int>(reader.which())
              << std::endl;
  }
}

