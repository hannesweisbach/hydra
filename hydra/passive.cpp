#include <chrono>
#include <typeinfo>

#include <unordered_map>
#include <algorithm>

#include "hash.h"
#include "passive.h"
#include "messages.h"
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
    : s(host, port), heap(48U, size2Class, s), local_heap(s),
      msg_buffer(local_heap.malloc<msg>(2)),
      info(local_heap.malloc<node_info>()),
      remote_table(nullptr), prefetch(1) {
  log_info() << "Starting client to " << host << ":" << port;
  post_recv(msg_buffer.first.get()[0], msg_buffer.second);
  post_recv(msg_buffer.first.get()[1], msg_buffer.second);

  s.connect();


  init_request request;
  auto future = request.set_completion<const mr &>([&](auto &&mr) {
    remote = mr;
    update_info();
  });

  s.sendImmediate(request);
  future.wait();
}

hydra::passive::passive(hydra::passive &&other)
    : s(std::move(other.s)), heap(std::move(other.heap)),
      local_heap(std::move(other.local_heap)),
      msg_buffer(std::move(other.msg_buffer)), info(std::move(other.info)),
      remote_table(std::move(other.remote_table)), prefetch(other.prefetch) {}

hydra::passive &hydra::passive::operator=(hydra::passive &&other) {
  std::swap(s, other.s);
  std::swap(heap, other.heap);
  std::swap(local_heap, other.local_heap);
  std::swap(msg_buffer, other.msg_buffer);
  std::swap(info, other.info);
  std::swap(prefetch, other.prefetch);
  std::swap(remote_table, other.remote_table);

  return *this;
}

hydra::passive::~passive() {
  disconnect_request request;
  //auto future = request.set_completion();
  s.sendImmediate(request);
  //future.get();
  s.disconnect();
}

std::future<void> hydra::passive::post_recv(const msg &m,
                                           const ibv_mr *mr) {
  auto fut = s.recv_async(m, mr);
#if 0
  return hydra::then(std::move(fut), [=,&m](auto m_) mutable {
    m_.get(); // check for exception
#else
  return messageThread.send([this, &m, mr, future=std::move(fut)]()mutable{
#endif
  future.get();
  recv(m);
  post_recv(m, mr);
});
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
  s.read(info.first.get(), info.second,
         reinterpret_cast<node_info *>(remote.addr), remote.rkey).get();
  log_info() << info.first->key_extents;
  remote_table = reinterpret_cast<key_entry *>(info.first->key_extents.addr);
  rkey = info.first->key_extents.rkey;
  table_size = info.first->table_size;
  log_debug() << "Remote table: " << table_size << " @" << (void *)remote_table
              << " (" << rkey << ")";
}

void hydra::passive::recv(const msg &r) {
  log_info() << r;

  switch (r.type()) {
  case msg::type::response:
    static_cast<const response &>(r).complete_();
    break;
  case msg::type::notification:
    switch (r.subtype()) {
    case msg::subtype::resize: {
      update_info();
#if 0
      std::thread t([&]() {
        std::unordered_map<uint64_t, uint64_t> distribution;
        size_t old_fail = 0;
        size_t fails = 0;
        size_t loads = 0;
        log_info() << "Starting mod test";
        while (1) {
          auto table = s.read<RDMAObj<routing_table> >(
              reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
              info.first->routing_table.rkey);
          loads++;
          table.first.get();

          if (!table.second.first->valid()) {
            fails++;
          } else {
            distribution[fails - old_fail]++;
            old_fail = fails;
          }
          if ((loads & 0xffff) == 0) {
            log_info() << "loads: " << loads << " fails: " << fails
                       << " ratio: " << (float)fails / loads;
            print_distribution(distribution);
          }
        }
      });
      t.detach();
#endif
    } break;
    default:
      break;
    }
    break;
  default:
    assert(false);
  }
}
  
hydra::routing_table hydra::passive::table() const {
  auto table = s.read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
      info.first->routing_table.rkey);
  table.first.get();
  return table.second.first->get();
}

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  notification_predecessor m(pred);
  s.sendImmediate(m);
}

void hydra::passive::send(const msg& m) const {
  s.sendImmediate(m);
}

void hydra::passive::update_table_entry(const hydra::node_id &pred,
                                       size_t entry) const {}

bool hydra::passive::has_id(const keyspace_t &id) const {
  auto table = s.read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
      info.first->routing_table.rkey);
  table.first.get();
  auto t = table.second.first->get();
  return id.in(t.self().node.id, t.successor().node.id);
//  return hydra::interval({t.self().node.id, t.successor().node.id}).contains(id);
}

bool hydra::passive::contains(const char *key, size_t key_length) {
  // TODO locate node, get node_info struct.
  size_t index = hash(key, key_length) % table_size;
  (log_debug() << "Searching for key ").write(key, key_length) << " " << key_length;
  log_info() << "index " << index << " " << hash(key, key_length);
  log_debug() << "Remote table: " << table_size << " " << (void*)remote_table << " ("
              << rkey << ")";
  // watch for wrapping hood
  // TODO build a nice iterator
  log_debug() << "Reading entry @" << std::hex << std::showbase
              << &remote_table[index];

#if 0
  auto v = s.read(&remote_table[index], rkey);
  auto &e = v.front();
  log_hexdump_ptr(&e, sizeof(e));
  log_debug() << "Pointer " << &e << " valid: " << e.valid()
              << " crc: " << e.crc;
  log_trace() << "Starting search with " << std::hex << std::showbase
              << (void *)e.key() << " " << e.rkey;
  for (size_t hop = e.hop, d = 0; hop; hop >>= 1, d++) {
    if ((hop & 1) && !e.is_empty() && (key_length == e.key_length())) {
      auto v = s.read(&remote_table[(index + d) % table_size], rkey);
      auto &e = v.front();
      log_trace() << e.valid() << " index " << index + d << " has pointer to "
                  << std::hex << std::showbase << e.ptr.ptr << " " << std::dec
                  << e.key_length() << " " << e.rkey;
      // TODO loop while e not valid.
      if (e.valid() && (e.key_length() == key_length)) {
        std::vector<unsigned char> remote_key;
        uint64_t hash__ = 0;
        do {
          uint8_t k[e.key_length()];
          log_debug() << e.ptr.ptr << " " << e.rkey << " " << e.key_length();
          s.read(k, reinterpret_cast<uint64_t>(e.ptr.ptr), e.rkey,
                 e.key_length());
          remote_key =
              s.read(const_cast<unsigned char *>(e.key()), e.rkey, e.ptr.size);
          hash__ = hash64(remote_key.data(), e.ptr.size);
          log_debug() << "hash " << e.ptr.crc << " " << hash__ << " "
                      << (void *)remote_key.data();
        } while (e.ptr.crc != hash__);
        (log_debug() << "Found key ").write(remote_key.data(), key_length);
        if (memcmp(remote_key.data(), key, key_length) == 0)
          return true;
      }
    }
  }

#else
  //auto entry = local_heap.malloc<unsigned char>(sizeof(key_entry));
  auto mem = local_heap.malloc<key_entry>();
  do {
    s.read(mem.first.get(), mem.second, &remote_table[index], rkey).get();
  } while (!mem.first->valid());
  auto &entry = mem.first;

  for (size_t hop = entry->hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry->is_empty() &&
        (key_length == entry->key_length())) {
      log_trace() << entry->valid() << " index " << index + d - 1
                  << " has pointer to " << std::hex << std::showbase
                  << entry->ptr.ptr << " " << std::dec << entry->key_length()
                  << " " << entry->rkey;
      auto data = local_heap.malloc<unsigned char>(entry->ptr.size);
      uint64_t hash__ = 0;
      do {
        log_debug() << entry->ptr.ptr << " " << entry->rkey << " " << entry->key_length();
        s.read(data.first.get(), data.second, entry->key(), entry->rkey,
               entry->ptr.size).get();
        hash__ = hash64(data.first.get(), entry->ptr.size);
        log_debug() << "hash " << entry->ptr.crc << " " << hash__ << " "
                    << (void *)data.first.get();
      } while (entry->ptr.crc != hash__);
      (log_debug() << "Found key ").write(data.first.get(), key_length);
      if (memcmp(data.first.get(), key, key_length) == 0)
        return true;
    }
    do {
      s.read(mem.first.get(), mem.second,
             &remote_table[(index + d) % table_size], rkey).get();
    } while (!mem.first->valid());
  }

#endif

  return false;
}

