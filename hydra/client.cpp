#include <chrono>
#include <typeinfo>

#include "hash.h"
#include "client.h"
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

hydra::client::client(const std::string &host, const std::string &port)
    : s(host, port), heap(48U, size2Class, s), local_heap(s),
      msg_buffer(local_heap.malloc<msg>(2)),
      info(local_heap.malloc<node_info>()), id(0xdeadbabe), prefetch(1),
      remote_table(nullptr) {
  auto f = post_recv(msg_buffer.first.get()[0], msg_buffer.second);
  post_recv(msg_buffer.first.get()[1], msg_buffer.second);
  s.connect();
  f.wait();
}

hydra::client::~client() {
  msg_disconnect m(id);
  s.sendImmediate(m);
}

std::future<void> hydra::client::post_recv(const msg &m, const ibv_mr *mr) {
  auto fut = s.recv_async(m, mr);
#if 0
  return hydra::then(std::move(fut), [=,&m](auto m_) mutable {
    m_.get(); // check for exception
#else
  return messageThread.send([this,&m, mr, future=std::move(fut)]()mutable{
#endif
    future.get();
    recv(m);
    post_recv(m, mr);
  });
}

void hydra::client::recv(const msg &msg) {
  log_info() << "FOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO new handler: " << msg.type();
  log_info() << msg;
  log_hexdump(msg);
  switch (msg.type()) {
  case INIT:
    id = msg.id();
    log_info() << "ID for remote " << msg.id();
    remote = msg_init(msg).init();
  [[clang::fallthrough]];
  case RESIZE: {
    log_info() << "remote mr: " << remote;
    s.read(info.first.get(), info.second,
           reinterpret_cast<node_info *>(remote.addr), remote.rkey).get();
    log_info() << info.first->key_extents;
    remote_table = reinterpret_cast<key_entry *>(info.first->key_extents.addr);
    rkey = info.first->key_extents.rkey;
    table_size = info.first->table_size;
    log_debug() << "Remote table: " << table_size << " @" << (void*)remote_table
                << " (" << rkey << ")";
  } break;
  case ACK: {
    msg_ack m(msg);
    log_info() << "Received ack: " << m;
    auto functor = reinterpret_cast<std::function<void(bool)> *>(m.cookie());
    if (functor)
      (*functor)(true);
  } break;
  default:
    break;
  }
  memset(const_cast<uint8_t*>(msg.data()), 'U', msg.size());
}

std::future<bool> hydra::client::add(const char *key, size_t key_length, const char *value,
                        size_t value_length) {
  // TODO maybe expose allocation functions, so that key and value are placed
  // into memory, allocated by heap.malloc()
  ibv_mr *key_mr = s.mapMemory(key, key_length);
  ibv_mr *value_mr = s.mapMemory(value, value_length);

  auto promise = std::make_shared<std::promise<bool>>();
  std::function<void(bool)> *cookie = new std::function<void(bool)>();
  *cookie = [=](bool ack) {
    rdma_dereg_mr(key_mr);
    rdma_dereg_mr(value_mr);
    promise->set_value(ack);
    delete cookie;
  };

  log_info() << key_mr;
  //log_info() << value_mr;

  msg_add data = { id,
                   reinterpret_cast<uint64_t>(cookie),
                   { key, key_length, key_mr->rkey },
                   { value, value_length, value_mr->rkey } };
  log_info() << data;
  s.sendImmediate(data);
  return promise->get_future();
}

std::future<bool> hydra::client::remove(const char * key, size_t key_length) {
  ibv_mr *key_mr = s.mapMemory(key, key_length);
  
  auto promise = std::make_shared<std::promise<bool>>();
  std::function<void(bool)> *cookie = new std::function<void(bool)>();
  *cookie = [=](bool ack) {
    rdma_dereg_mr(key_mr);
    promise->set_value(ack);
    delete cookie;
  };
  
  log_info() << key_mr;

  msg_del data(id, reinterpret_cast<uint64_t>(cookie),
               { key, key_length, key_mr->rkey });
  log_info() << data;
  s.sendImmediate(data);
  return promise->get_future();
}

bool hydra::client::contains(const char *key, size_t key_length) {
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

/* alloc: return managed array or std::unique_ptr<char[]>
 * or { char[], size, unqiue_ptr<ibv_mr> }
 * this should also play nice with read.
 */
hydra::client::value_ptr hydra::client::get(const char *key,
                                            const size_t key_length) {
  size_t index = hash(key, key_length) % table_size;

  auto mem = local_heap.malloc<key_entry>();
  do {
    s.read(mem.first.get(), mem.second, &remote_table[index], rkey).get();
  } while (!mem.first->valid());
  auto &entry = mem.first;

  for (size_t hop = entry->hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry->is_empty() &&
        (key_length == entry->key_length())) {
      auto data = local_heap.malloc<unsigned char>(entry->ptr.size);
      uint64_t crc = 0;
      do {
        s.read(data.first.get(), data.second, entry->key(), entry->rkey,
               entry->ptr.size).get();
        crc = hash64(data.first.get(), entry->ptr.size);
      } while (entry->ptr.crc != crc);
      if (memcmp(data.first.get(), key, key_length) == 0) {
        uint8_t *val = data.first.get() + entry->key_length();
        size_t length = entry->value_length();
        value_ptr result([=]() {
                           void *p = check_nonnull(malloc(length));
                           memcpy(p, val, length);
                           return reinterpret_cast<char *>(p);
                         }(),
                         std::function<void(char *)>(free));
        return result;
      }
    }
    do {
      s.read(mem.first.get(), mem.second,
             &remote_table[(index + d) % table_size], rkey).get();
    } while (!mem.first->valid());
  }

  return value_ptr(nullptr, [](void*){});
}
