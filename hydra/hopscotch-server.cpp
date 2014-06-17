#include <iostream>
#include <algorithm>

#include "util/utils.h"
#include "hash.h"
#include "Logger.h"
#include "hopscotch-server.h"

std::ostream &hydra::operator<<(std::ostream &s, const hydra::hash_table_entry &e) {
  s << std::hex << std::setfill('0') << std::setw(2) << (uint32_t)e.hop << std::dec
    << " ";
  s << (void*)e.key() << " (";
  if (e.key() != nullptr)
    s.write(reinterpret_cast<const char *>(e.key()),
            (std::streamsize)std::min(e.key_length(), static_cast<size_t>(16)));
  s << ") " << e.key_length() << " " << (void *)e.value() << " (";
  if (e.value() != nullptr)
    s.write(
        reinterpret_cast<const char *>(e.value()),
        (std::streamsize)std::min(e.value_length(), static_cast<size_t>(16)));
  s << ") " << e.rkey;
  return s;
}

size_t hydra::hopscotch_server::next_size() const {
  const size_t max_size =
      std::numeric_limits<hydra::keyspace_t::value_type>::max() + hop_range;
  assert(("Table cannot grow anymore.", table_size <= max_size));
  const size_t proposed_next_size = server_dht::next_size();
  return std::min(max_size, proposed_next_size);
}

size_t hydra::hopscotch_server::home_of(const hydra::server_dht::key_type &key) const {
  __uint128_t h = hash(key.first, key.second);
  return (h % table_size);
}

size_t hydra::hopscotch_server::next_free_index(size_t from) const {
  auto free = [](const auto &e) {
    e.lock();
    if (!e) // empty
      return true;
    e.unlock();
    return false;
  };

  return find_if(from, from, free);
}

size_t hydra::hopscotch_server::next_movable(size_t to) const {
  size_t start = (to - (hop_range - 1) + table_size) % table_size;
  for (size_t i = start; i != to; i = (i + 1) % table_size) {
    const size_t distance = (to - i + table_size) % table_size;
    /* racy */
    size_t hop = table[i].get().hop;
    for (size_t d = 0; hop; d++, hop >>= 1) {
      shadow_table[(i + d) % table_size].lock();
      if ((hop & 1) && (d < distance))
          return (i + d) % table_size;
      shadow_table[(i + d) % table_size].unlock();
    }
  }
  return invalid_index();
}

void
hydra::hopscotch_server::add(std::tuple<mem_type, size_t, size_t, uint32_t> &&e,
                             const size_t to, const size_t home) {
  assert(std::get<0>(e));
  //table[to] = hash_table_entry(e.key(), e.size, e.key_size, e.rkey, table[to].get().hop);
  shadow_table[to].set(std::move(std::get<0>(e)), std::get<1>(e), std::get<2>(e), std::get<3>(e));
  size_t distance = (to - home + table_size) % table_size;
  assert(distance < hop_range);
  /* racy */
  table[home]([=](auto &&entry) { entry.set_hop(distance); });
}

void hydra::hopscotch_server::move(size_t from, size_t to) {
  //log_info() << "Moving " << from " to " << to;
  const size_t home = home_of(table[from].get());
  //add(std::move(shadow_table[from]), to, home);
  shadow_table[to] = std::move(shadow_table[from]);
  size_t distance = (to - home + table_size) % table_size;
  assert(distance < hop_range);
  /* racy */
  table[home]([=](auto &&entry) { entry.set_hop(distance); });
  
  const size_t old_hops = (from - home + table_size) % table_size;
  assert(old_hops < hop_range);
  //TODO express in terms of delete
  table[home]([=](auto &&entry) { entry.clear_hop(old_hops); });
  //mark from as free.
  table[from]([](auto &&entry) { entry.empty(); });
  shadow_table[from].empty();
}

size_t hydra::hopscotch_server::move_into(size_t to) {
  size_t movable = next_movable(to);
  if(!index_valid(movable)) {
    shadow_table[to].unlock();
    return movable;
  }

  move(movable, to);
  shadow_table[to].unlock();
  return movable;
}

hydra::Return_t hydra::hopscotch_server::add(
    std::tuple<mem_type, size_t, size_t, uint32_t> &&e) {
  key_type key(std::get<0>(e).get(), std::get<2>(e));
  /* overwrite existing key */
  auto index = contains(key);
  if (index != invalid_index()) {
    add(std::move(e), index, home_of(key));
//    shadow_table[index].debug();
    shadow_table[index].unlock();
    return SUCCESS;
  }

  index = home_of(key);
  log_info() << "index: " << index << " " << (void*)e.key() << " " << e.key_size;
  log_info() << hash(e.key(), e.key_size) << " " << table_size << " " << hash64(e.key(), e.size);
  log_info() << hash(e.key(), e.key_size) % table_size;
  
  for (size_t next = next_free_index(index); index_valid(next);
       next = move_into(next)) {
    size_t distance = (next - index + table_size) % table_size;
    if (distance < hop_range) {
      add(std::move(e), next, index);
      dump();
      used++;
  //    shadow_table[next].debug();
      shadow_table[next].unlock();
      return SUCCESS;
    }
    shadow_table[next].unlock();
  }

  return NEED_RESIZE;
}

size_t hydra::hopscotch_server::contains(const key_type &key) {
  const size_t size = key.second;
  size_t start = home_of(key);
  size_t end = (start + hop_range) % table_size;
  const auto &home = shadow_table[start];
  size_t index = 0;

  auto has_key = [&](const auto &e) {
    if (!home.has_hop(index++))
      return false;

    e.lock();
    if (e.key_size() == size && e.has_key(key.first))
      return true;
    e.unlock();
    return false;
  };

  auto ret = find_if(start, end, has_key);
  return ret;
}

hydra::Return_t hydra::hopscotch_server::remove(const key_type &key) {
  for (size_t i = home_of(key), hop = table[i].get().hop; hop;
       i = (i + 1) % table_size, hop >>= 1) {
    size_t distance = (i - home_of(key) + table_size) % table_size;
    if ((hop & 1) && (distance < hop_range)) {
      shadow_table[i].lock();
      if (table[i].get().key_length() == key.second &&
          memcmp(table[i].get().key(), key.first, key.second) == 0) {
        table[home_of(key)]([=](auto && entry) { entry.clear_hop(distance); });
        table[i]([](auto &&entry) { entry.empty(); });
        shadow_table[i].empty();
        used--;
        shadow_table[i].unlock();
        return SUCCESS;
      }
      shadow_table[i].unlock();
    }
  }
  return NOTFOUND;
}

void hydra::hopscotch_server::dump() const {
  for (size_t i = 0; i < table_size; i++) {
    auto &e = shadow_table[i];
    if (e)
      std::cout << &e << " " << std::setw(6) << i << " " << e.rdma_entry.get()
                << std::endl;
  }
}

void hydra::hopscotch_server::check_consistency() const {
#ifndef NDEBUG
  for (size_t i = 0; i < table_size; i++) {
    const auto &shadow_entry = shadow_table[i];
    const auto &rdma_entry = table[i];
    if ((shadow_entry.mem.get() != rdma_entry.get().key()) ||
        (shadow_entry.rkey() != rdma_entry.get().rkey) ||
        (shadow_entry.size() != rdma_entry.get().ptr.size) ||
        (shadow_entry.key_size() != rdma_entry.get().key_size) ||
        (!rdma_entry.valid()) ||
        (rdma_entry.get().rkey == 0 && (rdma_entry.get().hop & 1))) {
      log_info() << i;
      log_info() << shadow_entry;
      log_info() << rdma_entry.get();
      dump();
      std::terminate();
    }
  }
#endif
}

