#include <iostream>
#include <algorithm>

#include <city.h>

#include "util/utils.h"
#include "hash.h"
#include "Logger.h"
#include "hydra/cuckoo-server.h"

#include "util/stat.h"

namespace hydra {
extern ecdf measurement;
}

size_t hydra::cuckoo_server::index(const key_type &key,
                                   const uint64_t seed) const {
  /* from 'const unsigned char *' to 'const char *' */
  auto ptr = reinterpret_cast<const char *>(key.first);
  const auto size = key.second;
  return CityHash64WithSeed(ptr, size, seed) % table_size;
}

void hydra::cuckoo_server::rehash() {
  ++rehash_count;
  for (auto &&seed : seeds) {
    seed = distribution(generator);
  }

  bool rehashing_ = rehashing;
  rehashing = true;

  for (auto &&entry : shadow_table) {
    if (entry) {
      auto tmp = std::make_tuple(std::move(entry.mem), entry.size(),
                                 entry.key_size(), entry.rkey());
      entry.empty();
      auto ret = add(tmp);
      if (ret != SUCCESS) {
        std::cerr << "Error during rehashing." << std::endl;
      }
    }
  }

  rehashing = rehashing_;
}

hydra::Return_t
hydra::cuckoo_server::add(std::tuple<mem_type, size_t, size_t, uint32_t> &e) {
  key_type key(std::get<0>(e).get(), std::get<2>(e));
  for (uint32_t seed_idx = 0; seed_idx < seeds.size(); ++seed_idx) {
    auto idx = index(key, seeds[seed_idx]);
    /* insert if empty or overwrite if same key */
    if (!shadow_table[idx]) {
      shadow_table[idx].swap(e, seed_idx);
      if(!rehashing) {
        ++used_;
      }
      return SUCCESS;
    } else if (shadow_table[idx].has_key(key)) {
      shadow_table[idx].swap(e, seed_idx);
      return SUCCESS;
    }
  }

  uint32_t seed_idx = 0;
  for (size_t i = 0; i < 32; ++i) {
    const key_type key(std::get<0>(e).get(), std::get<2>(e));
    const auto idx = index(key, seeds[seed_idx]);
    shadow_table[idx].swap(e, seed_idx);
    if (!std::get<0>(e)) {
      if (!rehashing) {
        ++used_;
      }
      return SUCCESS;
    }
    seed_idx = (seed_idx + 1) % seeds.size();
  }

  rehash();
  return add(e);
}

hydra::Return_t hydra::cuckoo_server::remove(const key_type &key) {
  const size_t kv = contains(key);
  if (kv == invalid_index())
    return NOTFOUND;

  shadow_table[kv].empty();
  used_--;

  return SUCCESS;
}

void hydra::cuckoo_server::resize(LocalRDMAObj<hash_table_entry> *new_table,
                                  size_t size) {
  log_info() << "Table size: " << size;
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
      auto ret = add(tmp);
      if (ret != SUCCESS) {
        std::cerr << "Error during resizing." << std::endl;
      }
    }
  }

  log_size = hydra::util::log2(table_size);
}

size_t hydra::cuckoo_server::contains(const key_type &key) {
  for (const auto &seed : seeds) {
    auto idx = index(key, seed);
    if (shadow_table[idx].has_key(key))
      return idx;
  }
  return invalid_index();
}

void hydra::cuckoo_server::dump() const { dump(0, table_size); }
void hydra::cuckoo_server::dump(const size_t &from, const size_t &to) const {
  for (size_t i = from; i < to; i++) {
    auto &e = shadow_table[i];
    if (e)
      std::cout << &e << " " << std::setw(6) << i << " " << e.entry.get()
                << std::endl;
  }
}

void hydra::cuckoo_server::check_consistency() const {
#ifndef NDEBUG
  for (size_t i = 0; i < table_size; i++) {
    const auto &shadow_entry = shadow_table[i];
    const auto &rdma_entry = table[i];
    if ((shadow_entry.mem.get() != rdma_entry.get().key()) ||
        (shadow_entry.rkey() != rdma_entry.get().rkey) ||
        (shadow_entry.size() != rdma_entry.get().ptr.size) ||
        (shadow_entry.key_size() != rdma_entry.get().key_size) ||
        (!rdma_entry.valid())) {
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

