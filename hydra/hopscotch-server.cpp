#include <iostream>
#include <algorithm>

#include "util/utils.h"
#include "hash.h"
#include "Logger.h"
#include "hopscotch-server.h"

std::ostream &hydra::operator<<(std::ostream &s, const hydra::key_entry &e) {
  s << std::hex << std::setfill('0') << std::setw(2) << (uint32_t)e.hop << std::dec
    << " ";
  s << (void*)e.key() << " (";
  if (e.key() != nullptr)
    s.write(reinterpret_cast<const char *>(e.key()),
            (std::streamsize)std::min(e.key_length(), static_cast<size_t>(16)));
  s << ") " << (void *)e.value() << " (";
  if (e.value() != nullptr)
    s.write(
        reinterpret_cast<const char *>(e.value()),
        (std::streamsize)std::min(e.value_length(), static_cast<size_t>(16)));
  s << ")";
  return s;
}

size_t hydra::hopscotch_server::home_of(const hydra::server_dht::key_type &key) const {
  __uint128_t h = hash(key.first, key.second);
  return (h % table_size);
}

size_t hydra::hopscotch_server::next_free_index(size_t from) const {
  if (table[from].is_empty())
    return from;
  for (size_t i = (from + 1) % table_size; i != from;
       i = (i + 1) % table_size) {
    if (table[i].is_empty()) {
      return i;
    }
  }
  return invalid_index();
}

size_t hydra::hopscotch_server::next_movable(size_t to) const {
  size_t start = (to - (hop_range - 1) + table_size) % table_size;
  for (size_t i = start; i != to; i = (i + 1) % table_size) {
    const size_t distance = (to - i + table_size) % table_size;
    size_t hop = table[i].hop;
    for (size_t d = 0; hop; d++, hop >>= 1) {
      if ((hop & 1) && (d < distance))
          return (i + d) % table_size;
    }
  }
  return invalid_index();
}

void hydra::hopscotch_server::add(hydra::hopscotch_server::resource_entry&& e, const size_t to, const size_t home) {
  table[to] = key_entry(e.key(), e.size, e.key_size, e.rkey);
  size_t distance = (to - home + table_size) % table_size;
  assert(distance < hop_range);
  table[home].set_hop(distance);
  shadow_table[to] = std::move(e);
  used++;
}

void hydra::hopscotch_server::move(size_t from, size_t to) {
  //log_info() << "Moving " << from " to " << to;
  const size_t home = home_of(table[from]);
  add(std::move(shadow_table[from]), to, home);
  
  const size_t old_hops = (from - home + table_size) % table_size;
  assert(old_hops < hop_range);
  //TODO express in terms of delete
  table[home].clear_hop(old_hops);
  //mark from as free.
  table[from].empty();
}

size_t hydra::hopscotch_server::move_into(size_t to) {
  size_t movable = next_movable(to);
  if(!index_valid(movable))
    return movable;

  move(movable, to);
  return movable;
}

hydra::Return_t hydra::hopscotch_server::add(hydra::hopscotch_server::resource_entry&& e) {
  key_type key(e.key(), e.key_size);
  if (contains(key) != invalid_index()) {
    add(std::move(e), contains(key), home_of(key));
    return SUCCESS;
  }
  size_t index = home_of(key);

  log_info() << "index: " << index << " " << (void*)e.key() << " " << e.key_size;
  log_info() << hash(e.key(), e.key_size) << " " << table_size << " " << hash64(e.key(), e.size);
  log_info() << hash(e.key(), e.key_size) % table_size;
  
  for (size_t next = next_free_index(index); index_valid(next);
       next = move_into(next)) {
    size_t distance = (next - index + table_size) % table_size;
    if (distance < hop_range) {
      add(std::move(e), next, index);
      dump();
      return SUCCESS;
    }
  }

  return NEED_RESIZE;
}

size_t hydra::hopscotch_server::contains(const key_type &key) {
  const size_t size = key.second;
  for (size_t i = home_of(key), hop = table[i].hop; hop;
       i = (i + 1) % table_size, hop >>= 1) {
    size_t distance = (i - home_of(key) + table_size) % table_size;
    if ((hop & 1) && (distance < hop_range)) {
      if (table[i].key_length() == size &&
          memcmp(table[i].key(), key.first, size) == 0) {
        return i;
      }
    }
  }
  return invalid_index();
}

hydra::Return_t hydra::hopscotch_server::remove(const key_type &key) {
  for (size_t i = home_of(key), hop = table[i].hop; hop;
       i = (i + 1) % table_size, hop >>= 1) {
    size_t distance = (i - home_of(key) + table_size) % table_size;
    if ((hop & 1) && (distance < hop_range)) {
      if (table[i].key_length() == key.second &&
          memcmp(table[i].key(), key.first, key.second) == 0) {
        table[home_of(key)].clear_hop(distance);
        table[i].empty();
        shadow_table[i].empty();
        used--;
        return SUCCESS;
      }
    }
  }
  return NOTFOUND;
}

void hydra::hopscotch_server::dump() const {
  for (size_t i = 0; i < table_size; i++) {
    auto &e = table[i];
    //auto &r = shadow_table[i];
    // if(!e.is_empty())
    log_info() << &e << " " << std::setw(2) << i << " " << e << " " << e.valid()
               << " " << e.crc << " " << e.rkey;
#if 0
    log_info() << (void*)r.mem.get() << " " << r.size << " " << r.key_size << " "
               << r.rkey;
#endif
  }
}

void hydra::hopscotch_server::check_consistency() const {
  for (size_t i = 0; i < table_size; i++) {
    auto& shadow_entry = shadow_table[i];
    auto& rdma_entry = table[i];
    assert(shadow_entry.mem.get() == rdma_entry.key());
    assert(shadow_entry.rkey == rdma_entry.rkey);
    assert(shadow_entry.size == rdma_entry.ptr.size);
    assert(shadow_entry.key_size == rdma_entry.key_size);
    assert(rdma_entry.valid());
  }
}
