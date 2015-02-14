#include <iostream>
#include <algorithm>

#include "util/utils.h"
#include "hash.h"
#include "Logger.h"
#include "hopscotch-server.h"

std::ostream &hydra::operator<<(std::ostream &s,
                                const hydra::hash_table_entry &e) {
  s << std::hex << std::setfill('0') << std::setw(2) << (uint32_t)e.hop
    << std::dec << " ";
  s << (void *)e.key() << " (";
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
  constexpr size_t max_size =
      std::numeric_limits<hydra::keyspace_t::value_type>::max();
  if (max_size < table_size - hop_range) {
    std::cout << "Table growing beyond maximum possible size: " << max_size
              << " " << table_size << std::endl;
    assert(("Table cannot grow anymore.", table_size <= max_size));
  }
  const size_t proposed_next_size = server_dht::next_size();
  return std::min(max_size, proposed_next_size);
}

size_t
hydra::hopscotch_server::home_of(const hydra::server_dht::key_type &key) const {
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
    const size_t home = home_of(table[i].get());
    const size_t distance = (to - i + table_size) % table_size;
    /* racy */
    for (size_t offset = 0; offset < distance; offset++) {
      if (shadow_table[i].has_hop(offset)) {
        return i + offset;
      }
    }
  }
  return invalid_index();
}

void
hydra::hopscotch_server::add(std::tuple<mem_type, size_t, size_t, uint32_t> &e,
                             const size_t to, const size_t home) {
  size_t distance = (to - home + table_size) % table_size;
  assert(distance < hop_range);
  assert(std::get<0>(e));
  shadow_table[to].set(shadow_table[home], distance, std::move(std::get<0>(e)),
                       std::get<1>(e), std::get<2>(e), std::get<3>(e));
}

void hydra::hopscotch_server::move(size_t from, size_t to) {
  // log_info() << "Moving " << from " to " << to;
  const size_t home = home_of(table[from].get());
  const size_t distance = (to - home + table_size) % table_size;
  const size_t old_hops = (from - home + table_size) % table_size;

  assert(distance < hop_range);
  assert(old_hops < hop_range);

  /* racy - move/hop update */
  // add(std::move(shadow_table[from]), to, home);
  shadow_table[to].into(std::move(shadow_table[from]), shadow_table[home],
                        old_hops, distance);
}

size_t hydra::hopscotch_server::move_into(size_t to) {
  size_t movable = next_movable(to);
  if (!index_valid(movable)) {
    shadow_table[to].unlock();
    return movable;
  }

  move(movable, to);
  shadow_table[to].unlock();
  return movable;
}

hydra::Return_t hydra::hopscotch_server::add(
    std::tuple<mem_type, size_t, size_t, uint32_t> &e) {
  key_type key(std::get<0>(e).get(), std::get<2>(e));
  /* overwrite existing key */
  auto index = contains(key);
  if (index != invalid_index()) {
    add(e, index, home_of(key));
    shadow_table[index].unlock();
    return SUCCESS;
  }

  index = home_of(key);
  for (size_t next = next_free_index(index); index_valid(next);
       next = move_into(next)) {
    size_t distance = (next - index + table_size) % table_size;
    if (distance < hop_range) {
      add(e, next, index);
      used_++;
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
    if (e.has_key(key))
      return true;
    e.unlock();
    return false;
  };

  auto ret = find_if(start, end, has_key);
  return ret;
}

hydra::Return_t hydra::hopscotch_server::remove(const key_type &key) {
  const size_t kv = contains(key);
  if (kv == invalid_index())
    return NOTFOUND;

  const size_t home = home_of(key);
  const size_t distance = (kv - home + table_size) % table_size;
  shadow_table[kv].empty(shadow_table[home], distance);
  shadow_table[kv].unlock();
  used_--;

  return SUCCESS;
}

void hydra::hopscotch_server::resize(LocalRDMAObj<hash_table_entry> *new_table,
                                     size_t size) {
  table_size = size;
  table = new_table;

  std::vector<resource_entry> tmp_shadow_table;
  tmp_shadow_table.reserve(table_size);

  for (size_t i = 0; i < table_size; i++) {
    new (&table[i]) LocalRDMAObj<hash_table_entry>;
    tmp_shadow_table.emplace_back(table[i]);
  }

  std::swap(shadow_table, tmp_shadow_table);
  used_ = 0;

  for (auto &&entry : tmp_shadow_table) {
    if (entry) {
      auto tmp = std::make_tuple(std::move(entry.mem), entry.size(),
                                 entry.key_size(), entry.rkey());
      add(tmp);
    }
  }
}

void hydra::hopscotch_server::dump() const { dump(0, table_size); }
void hydra::hopscotch_server::dump(const size_t &from, const size_t &to) const {
  for (size_t i = from; i < to; i++) {
    auto &e = shadow_table[i];
    if (e)
      std::cout << &e << " " << std::setw(6) << i << " " << e.entry.get()
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
      std::cout << i << " " << shadow_entry << std::endl;
      std::cout << std::boolalpha << "valid: " << rdma_entry.valid()
                << std::endl;
      std::cout << rdma_entry.get() << std::endl;
      dump();
      std::terminate();
    }
  }
#endif
}

