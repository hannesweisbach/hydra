#include <chrono>
#include <typeinfo>

#include <unordered_map>
#include <algorithm>

#include "hash.h"
#include "passive.h"
#include "client.h"
#include "util/Logger.h"
#include "util/concurrent.h"

auto size2Class = [](size_t size) -> size_t {
  if (size == 0)
    return 0;
  if (size <= 128) // 16 bins
    return ((size + (8 - 1)) & ~(8 - 1)) / 8 - 1;
  else if (size <= 4096) // 31 bins
    return ((size + (128 - 1)) & ~(128 - 1)) / 128 + 14;
  else
    return 47 + hydra::util::log2(size - 1) -
           hydra::util::static_log2<4096>::value;
};

hydra::passive::passive(const std::string &host, const std::string &port)
    : RDMAClientSocket(host, port), buffer(std::make_unique<buffer_t>()),
      buffer_mr(register_memory(ibv_access::MSG, *buffer)),
      heap(48U, size2Class, *this), info(std::make_unique<hydra::node_info>()),
      info_mr(register_memory(ibv_access::MSG, *info)),
      response(std::make_unique<response_t>()),
      response_mr(register_memory(ibv_access::MSG, *response)) {
  log_info() << "Starting client to " << host << ":" << port;

  connect();
}

bool hydra::passive::put(const std::vector<unsigned char> &kv,
                         const size_t &key_size) {
  using namespace hydra::rdma;
  auto future = recv_async(*response, response_mr.get());

  if (kv.size() < 256) {
    auto put = put_message_inline(kv, key_size);
    memcpy(std::begin(*buffer), std::begin(put),
           std::min(buffer->size() * sizeof(capnp::word), size_of(put)));
    send(*buffer, buffer_mr.get());

    future.get();
  } else {
    auto kv_mr = heap.malloc<unsigned char>(kv.size());
    memcpy(kv_mr.first.get(), kv.data(), kv.size());

    auto put = put_message(kv_mr, kv.size(), key_size);
    memcpy(std::begin(*buffer), std::begin(put),
           std::min(buffer->size() * sizeof(capnp::word), size_of(put)));
    send(*buffer, buffer_mr.get());

    future.get(); // stay in scope for kv_mr
  }

  auto message = capnp::FlatArrayMessageReader(*response);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
}

bool hydra::passive::remove(const std::vector<unsigned char> &key) {
  using namespace hydra::rdma;
  auto future = recv_async(*response, response_mr.get());

  if (key.size() < 256) {
    auto del = del_message_inline(key);
    memcpy(std::begin(*buffer), std::begin(del),
           std::min(buffer->size() * sizeof(capnp::word), size_of(del)));
    send(*buffer, buffer_mr.get());

    future.get();
  } else {
    auto key_mr = heap.malloc<unsigned char>(key.size());
    memcpy(key_mr.first.get(), key.data(), key.size());

    auto del = del_message(key_mr, key.size());
    memcpy(std::begin(*buffer), std::begin(del),
           std::min(buffer->size() * sizeof(capnp::word), size_of(del)));
    send(*buffer, buffer_mr.get());

    future.get(); // stay in scope for key_mr
  }

  auto message = capnp::FlatArrayMessageReader(*response);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
}

std::vector<unsigned char>
hydra::passive::find_entry(const std::vector<unsigned char> &key) {
  std::vector<unsigned char> value;
  update_info();
  const size_t table_size = info->table_size;
  const RDMAObj<hash_table_entry> *remote_table =
      static_cast<const RDMAObj<hash_table_entry> *>(info->key_extents.addr);
  const uint32_t rkey = info->key_extents.rkey;

  const size_t index = hash(key) % table_size;

  auto mem = from(&remote_table[index], info->key_extents.rkey);
  auto &entry = mem.first->get();

  for (size_t hop = entry.hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry.is_empty() && (key.size() == entry.key_length())) {
      auto data = heap.malloc<unsigned char>(entry.ptr.size);
      uint64_t crc = 0;
      do {
        read(data.first.get(), data.second, entry.key(), entry.rkey,
             entry.ptr.size).get();
        crc = hash64(data.first.get(), entry.ptr.size);
      } while (entry.ptr.crc != crc);
      if (std::equal(std::begin(key), std::end(key), data.first.get())) {
        value.insert(std::end(value), data.first.get() + entry.key_length(),
                     data.first.get() + entry.key_length() +
                         entry.value_length());
        return value;
      }
    }
    const size_t next_index = (index + d) % table_size;
    reload(mem, &remote_table[next_index], rkey);
  }

  return value;
}

bool hydra::passive::contains(const std::vector<unsigned char> &key) {
  return !find_entry(key).empty();
}

std::vector<unsigned char>
hydra::passive::get(const std::vector<unsigned char> &key) {
  return find_entry(key);
}

size_t hydra::passive::table_size() {
  update_info();
  return info->table_size;
}

#if 0
void print_distribution(std::unordered_map<uint64_t, uint64_t> &distribution) {
  std::vector<std::pair<uint64_t, uint64_t> > v(std::begin(distribution),
                                                std::end(distribution));
  std::sort(std::begin(v), std::end(v),
            [](auto rhs, auto lhs) { return rhs.first < lhs.first; });
  for (auto &&e : v) {
    log_info() << e.first << ": " << e.second;
  }
}
#endif

void hydra::passive::init() {
  auto future = recv_async(*response, response_mr.get());
  send(init_message());

  future.get();

  auto reply = capnp::FlatArrayMessageReader(*response);
  auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

  assert(reader.which() == hydra::protocol::DHTResponse::INIT);

  auto mr = reader.getInit().getInfo();
  assert(mr.getSize() >= sizeof(hydra::node_info));

  remote.addr = mr.getAddr();
  remote.size = mr.getSize();
  remote.rkey = mr.getRkey();
}

void hydra::passive::update_info() {
  if(remote.addr == 0)
    init();
  read(info.get(), info_mr.get(), reinterpret_cast<node_info *>(remote.addr),
       remote.rkey).get();
}

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  //no reply.
  send(predecessor_message(pred));
}

bool hydra::passive::has_id(const keyspace_t &id) const {
  auto table = read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info->routing_table.addr),
      info->routing_table.rkey);
  table.first.get();
  auto t = table.second.first->get();
  return id.in(t.self().node.id, t.successor().node.id);
//  return hydra::interval({t.self().node.id, t.successor().node.id}).contains(id);
}

