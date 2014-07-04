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
  auto key = remote.initKey();
  key.setAddr(0xdeadbabe);
  key.setSize(0x900d515e);
  key.setRkey(0xbeef);
  auto value = remote.initValue();
  value.setAddr(0xdeaddabf);
  value.setSize(0x900d515f);
  value.setRkey(0xbef0);

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
      auto key_reader = remote.getKey();
      std::cout << "key: " << key_reader.getAddr() << " "
                << key_reader.getSize() << " " << key_reader.getRkey()
                << std::endl;
      auto val_reader = remote.getValue();
      std::cout << "val: " << val_reader.getAddr() << " "
                << val_reader.getSize() << " " << val_reader.getRkey()
                << std::endl;
    } else {
      std::cout << "inline" << std::endl;
    }
  } else {
    std::cout << "unhandled message type " << static_cast<int>(reader.which())
              << std::endl;
  }
}

