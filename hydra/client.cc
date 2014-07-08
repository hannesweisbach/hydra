#include "hydra/client.h"
#include "hydra/chord.h"
#include "hydra/hash.h"

#include "util/Logger.h"

#include "hydra/protocol/message.h"

namespace hydra {
hydra::node_info get_info(const RDMAClientSocket &socket) {
  auto init = socket.recv_async<kj::FixedArray<capnp::word, 9> >();

  kj::Array<capnp::word> serialized = init_message();

  socket.sendImmediate(std::begin(serialized),
                       serialized.size() * sizeof(capnp::word));

  init.first.get(); // block.

  auto reply = capnp::FlatArrayMessageReader(*init.second.first);
  auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

  assert(reader.which() == hydra::protocol::DHTResponse::INIT);

  auto mr = reader.getInit().getInfo();
  assert(mr.getSize() >= sizeof(hydra::node_info));
  auto info = socket.read<hydra::node_info>(mr.getAddr(), mr.getRkey());
  info.first.get(); // block

  return *info.second.first;
}

static bool add(const RDMAClientSocket &socket,
                const rdma_ptr<unsigned char> &kv, const size_t &size,
                const size_t &key_size) {
  auto response = socket.recv_async<kj::FixedArray<capnp::word, 9> >();
  auto put = put_message(kv, size, key_size);

  socket.sendImmediate(put);

  response.first.get();

  auto message = capnp::FlatArrayMessageReader(*response.second.first);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
}

bool add(const RDMAClientSocket &socket, const std::vector<unsigned char> &kv,
         const size_t &key_size) {
  auto kv_mr = socket.malloc<unsigned char>(kv.size());

  memcpy(kv_mr.first.get(), kv.data(), kv.size());
  return add(socket, kv_mr, kv.size(), key_size);
}

bool add(const RDMAClientSocket &socket, const std::vector<unsigned char> &key,
         const std::vector<unsigned char> &value) {
  const size_t size = key.size() + value.size();
  auto kv_mr = socket.malloc<unsigned char>(size);

  memcpy(kv_mr.first.get(), key.data(), key.size());
  memcpy(kv_mr.first.get() + key.size(), value.data(), value.size());
  return add(socket, kv_mr, size, key.size());
}

bool remove(const RDMAClientSocket &socket,
            const std::vector<unsigned char> &key) {
  auto key_mr = socket.malloc<unsigned char>(key.size());

  memcpy(key_mr.first.get(), key.data(), key.size());

  auto response = socket.recv_async<kj::FixedArray<capnp::word, 9> >();
  auto del = del_message(key_mr, key.size());
  socket.sendImmediate(del);

  response.first.get();

  auto message = capnp::FlatArrayMessageReader(*response.second.first);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
}
}

hydra::client::client(const std::string &ip, const std::string &port)
    : root_node(ip, port) {}

hydra::node_id
hydra::client::responsible_node(const std::vector<unsigned char> &key) const {
  return chord::successor(root_node.table(), keyspace_t(hydra::hash(key)));
}

hydra::node_info hydra::client::get_info(const RDMAClientSocket &socket) const {
  return ::hydra::get_info(socket);
}

std::pair<hydra::client::value_ptr, const size_t>
hydra::client::find_entry(const RDMAClientSocket &socket,
                          const std::vector<unsigned char> &key) const {
  const hydra::node_info info = get_info(socket);
  const size_t table_size = info.table_size;
  const RDMAObj<hash_table_entry> *remote_table =
      static_cast<const RDMAObj<hash_table_entry> *>(info.key_extents.addr);
  const uint32_t rkey = info.key_extents.rkey;

  const size_t index = hash(key) % table_size;

  auto mem = socket.from(&remote_table[index], info.key_extents.rkey);
  auto &entry = mem.first->get();

  for (size_t hop = entry.hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry.is_empty() &&
        (key.size() == entry.key_length())) {
      auto data = socket.malloc<unsigned char>(entry.ptr.size);
      uint64_t crc = 0;
      do {
        socket.read(data.first.get(), data.second, entry.key(), entry.rkey,
                    entry.ptr.size).get();
        crc = hash64(data.first.get(), entry.ptr.size);
      } while (entry.ptr.crc != crc);
      if (std::equal(std::begin(key), std::end(key), data.first.get())) {
        return {std::move(data.first), entry.value_length()};
      }
    }
    const size_t next_index = (index + d) % table_size;
    socket.reload(mem, &remote_table[next_index], rkey);
  }

  return {value_ptr(nullptr, [](void *) {}), 0};
}

hydra::routing_table hydra::client::table() const { return root_node.table(); }

std::future<bool>
hydra::client::add(const std::vector<unsigned char> &key,
                   const std::vector<unsigned char> &value) const {
#if 1
  return hydra::async([=]() {
#else
  std::promise<bool> promise;
#endif
    auto start = std::chrono::high_resolution_clock::now();
    const auto nodeid = responsible_node(key);
    auto end = std::chrono::high_resolution_clock::now();
    // log_info() << "Blocked for "
    //           << std::chrono::duration_cast<std::chrono::microseconds>(
    //                  end - start).count();
    const RDMAClientSocket socket(nodeid.ip, nodeid.port);
    socket.connect();
    bool success = ::hydra::add(socket, key, value);
#if 1
    return success;
  });
#else
  promise.set_value(success);
  return promise.get_future();
#endif
}

std::future<bool>
hydra::client::remove(const std::vector<unsigned char> &key) const {
  return hydra::async([=]() {
    const auto nodeid = responsible_node(key);
    const RDMAClientSocket socket(nodeid.ip, nodeid.port);
    socket.connect();

    auto response = socket.recv_async<remove_response>();

    auto key_mr = socket.malloc<unsigned char>(key.size());
    memcpy(key_mr.first.get(), key.data(), key.size());

    remove_request request = { { key_mr.first.get(), key.size(),
                                 key_mr.second->rkey } };

    socket.sendImmediate(request);

    response.first.get();
    return response.second.first->value();
  });
}

bool hydra::client::contains(const std::vector<unsigned char> &key) const {
  const auto nodeid = responsible_node(key);
  const RDMAClientSocket socket(nodeid.ip, nodeid.port);
  socket.connect();
  const auto entry = find_entry(socket, key);
  return entry.first.get() != nullptr;
}

/* alloc: return managed array or std::unique_ptr<char[]>
 * or { char[], size, unqiue_ptr<ibv_mr> }
 * this should also play nice with read.
 */
hydra::client::value_ptr
hydra::client::get(const std::vector<unsigned char> &key) const {
  const auto nodeid = responsible_node(key);
  // TODO: guard socket connect/disconnect
  const RDMAClientSocket socket(nodeid.ip, nodeid.port);
  socket.connect();
  const size_t key_size = key.size();
  auto entry = find_entry(socket, key);

  if (entry.first.get() == nullptr) {
    return std::move(entry.first);
  } else {
    value_ptr result([=, &entry]() {
                       const size_t length = entry.second;
                       void *p = check_nonnull(::malloc(length));
                       memcpy(p, entry.first.get() + key_size, length);
                       return reinterpret_cast<unsigned char *>(p);
                     }(),
                     std::function<void(unsigned char *)>(free));
    return result;
  }
}

