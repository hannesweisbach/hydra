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
    : RDMAClientSocket(host, port),
      buffer_mr(register_memory(ibv_access::MSG, *buffer)),
      heap(48U, size2Class, *this), local_heap(*this),
      info(local_heap.malloc<node_info>()) {
  log_info() << "Starting client to " << host << ":" << port;

  connect();
}

bool hydra::passive::put(const std::vector<unsigned char> &kv,
                         const size_t &key_size) const {
  using namespace hydra::rdma;
  auto response = recv_async<kj::FixedArray<capnp::word, 9> >();

  if (kv.size() < 256) {
    auto put = put_message_inline(kv, key_size);
    memcpy(std::begin(*buffer), std::begin(put),
           std::min(buffer->size(), size_of(put)));
    send(*buffer, buffer_mr.get());

    response.first.get();
  } else {
    auto kv_mr = malloc<unsigned char>(kv.size());
    memcpy(kv_mr.first.get(), kv.data(), kv.size());

    auto put = put_message(kv_mr, kv.size(), key_size);
    memcpy(std::begin(*buffer), std::begin(put),
           std::min(buffer->size(), size_of(put)));
    send(*buffer, buffer_mr.get());

    response.first.get(); // stay in scope for kv_mr
  }

  auto message = capnp::FlatArrayMessageReader(*response.second.first);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
}

bool hydra::passive::remove(const std::vector<unsigned char> &key) const {
  using namespace hydra::rdma;
  auto response = recv_async<kj::FixedArray<capnp::word, 9> >();

  if (key.size() < 256) {
    auto del = del_message_inline(key);
    memcpy(std::begin(*buffer), std::begin(del),
           std::min(buffer->size(), size_of(del)));
    send(*buffer, buffer_mr.get());

    response.first.get();
  } else {
    auto key_mr = malloc<unsigned char>(key.size());
    memcpy(key_mr.first.get(), key.data(), key.size());

    auto del = del_message(key_mr, key.size());
    memcpy(std::begin(*buffer), std::begin(del),
           std::min(buffer->size(), size_of(del)));
    send(*buffer, buffer_mr.get());

    response.first.get(); // stay in scope for key_mr
  }

  auto message = capnp::FlatArrayMessageReader(*response.second.first);
  auto reader = message.getRoot<hydra::protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::ACK);
  return reader.getAck().getSuccess();
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

void hydra::passive::update_info() {
  log_info() << "remote mr: " << remote;
  read(info.first.get(), info.second,
         reinterpret_cast<node_info *>(remote.addr), remote.rkey).get();
}

hydra::routing_table hydra::passive::table() const {
  auto routing_mr = hydra::get_info(*this).routing_table;
  auto table = read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(routing_mr.addr), routing_mr.rkey);
  table.first.get();
  return table.second.first->get();
}

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  notification_predecessor m(pred);
  send(m);
}

bool hydra::passive::has_id(const keyspace_t &id) const {
  auto table = read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
      info.first->routing_table.rkey);
  table.first.get();
  auto t = table.second.first->get();
  return id.in(t.self().node.id, t.successor().node.id);
//  return hydra::interval({t.self().node.id, t.successor().node.id}).contains(id);
}

