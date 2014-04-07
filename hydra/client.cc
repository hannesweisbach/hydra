#include "hydra/client.h"
#include "hydra/chord.h"
#include "hydra/hash.h"

#include "util/Logger.h"

hydra::client::client(const std::string &ip, const std::string &port)
    : root_node(ip, port) {}

hydra::node_id hydra::client::responsible_node(const unsigned char *key,
                                               const size_t length) const {
  return chord::successor(
      root_node.table(),
      static_cast<hydra::keyspace_t::value_type>(hydra::hash(key, length)));
}

hydra::node_info hydra::client::get_info(const RDMAClientSocket& socket) const {
  auto init = socket.recv_async<mr_response>();
  init_request request;
  socket.sendImmediate(request);

  init.first.get(); //block.
  auto mr = init.second.first->value();
  assert(mr.size >= sizeof(hydra::node_info));
  auto info = socket.read<hydra::node_info>(mr.addr, mr.rkey);
  info.first.get(); //block

  //TODO return unique_ptr
  return *info.second.first;
}

std::pair<hydra::client::value_ptr, const size_t>
hydra::client::find_entry(const RDMAClientSocket &socket,
                          const unsigned char *key,
                          const size_t key_length) const {
  const hydra::node_info info = get_info(socket);
  const size_t table_size = info.table_size;
  key_entry *remote_table = static_cast<key_entry *>(info.key_extents.addr);
  const uint32_t rkey = info.key_extents.rkey;

  const size_t index = hash(key, key_length) % table_size;

  auto mem = socket.malloc<key_entry>();
  do {
    socket.read(mem.first.get(), mem.second, &remote_table[index], rkey).get();
  } while (!mem.first->valid());
  auto &entry = mem.first;

  for (size_t hop = entry->hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry->is_empty() &&
        (key_length == entry->key_length())) {
      auto data = socket.malloc<unsigned char>(entry->ptr.size);
      uint64_t crc = 0;
      do {
        socket.read(data.first.get(), data.second, entry->key(), entry->rkey,
                    entry->ptr.size).get();
        crc = hash64(data.first.get(), entry->ptr.size);
      } while (entry->ptr.crc != crc);
      if (memcmp(data.first.get(), key, key_length) == 0) {
        //auto p = std::move(data.first);
        return {std::move(data.first), entry->value_length()};
      }
    }
    do {
      socket.read(mem.first.get(), mem.second,
                  &remote_table[(index + d) % table_size], rkey).get();
    } while (!mem.first->valid());
  }

  return {value_ptr(nullptr, [](void *) {}), 0};
}

std::future<bool> hydra::client::add(const unsigned char *key, const size_t key_length,
                        const unsigned char *value, const size_t value_length) const {
  return hydra::async([=]() {
    auto nodeid = responsible_node(key, key_length);
    RDMAClientSocket socket(nodeid.ip, nodeid.port);
    socket.connect();
    auto response = socket.recv_async<put_response>();

    auto key_mr = socket.malloc<char>(key_length);
    auto val_mr = socket.malloc<char>(value_length);

    memcpy(key_mr.first.get(), key, key_length);
    memcpy(val_mr.first.get(), value, value_length);

    put_request request = {
      { key_mr.first.get(), key_length, key_mr.second->rkey },
      { val_mr.first.get(), value_length, val_mr.second->rkey }
    };

    socket.sendImmediate(request);

    log_info() << request;

    /* The blocking time depends on the remote node. TODO: Benchmark. */
    response.first.get(); // block.
    socket.sendImmediate(disconnect_request());
    return response.second.first->value();
  });
}

std::future<bool> hydra::client::remove(const unsigned char *key,
                                        const size_t key_length) const {
  const auto nodeid = responsible_node(key, key_length);
  auto node = std::make_shared<passive>(nodeid.ip, nodeid.port);
  auto key_mr = node->remote_from(key, key_length);

  remove_request request = { { key_mr.first.get(), key_length,
                               key_mr.second->rkey } };

  std::shared_ptr<unsigned char> key_ptr = std::move(key_mr.first);
  auto future = request.set_completion<bool>([=](bool) {
    (void)(key_ptr);
    (void)(node);
  });

  node->send(request);

  return future;
}

bool hydra::client::contains(const unsigned char *key,
                             const size_t key_length) const {
  const auto nodeid = responsible_node(key, key_length);
  const RDMAClientSocket socket(nodeid.ip, nodeid.port);
  socket.connect();
  const auto entry = find_entry(socket, key, key_length);
  socket.sendImmediate(disconnect_request());
  socket.disconnect();
  return entry.first.get() != nullptr;
}

/* alloc: return managed array or std::unique_ptr<char[]>
 * or { char[], size, unqiue_ptr<ibv_mr> }
 * this should also play nice with read.
 */
hydra::client::value_ptr hydra::client::get(const unsigned char *key,
                                            const size_t key_length) const {
  const auto nodeid = responsible_node(key, key_length);
  // TODO: guard socket connect/disconnect
  const RDMAClientSocket socket(nodeid.ip, nodeid.port);
  socket.connect();
  auto entry = find_entry(socket, key, key_length);
  socket.sendImmediate(disconnect_request());
  socket.disconnect();

  if (entry.first.get() == nullptr) {
    return std::move(entry.first);
  } else {
    value_ptr result([=, &entry]() {
                       const size_t length = entry.second;
                       void *p = check_nonnull(::malloc(length));
                       memcpy(p, entry.first.get() + key_length, length);
                       return reinterpret_cast<unsigned char *>(p);
                     }(),
                     std::function<void(unsigned char *)>(free));
    return result;
  }
}

