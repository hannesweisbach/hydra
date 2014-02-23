#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <functional>
#include <vector>

#include "types.h"

namespace hydra {
class server_dht {
protected:
  typedef unsigned char value_type;
  typedef std::unique_ptr<value_type, std::function<void(value_type *)> >
  mem_type;
  typedef std::unique_ptr<ibv_mr, std::function<void(ibv_mr *)> > mr_type;
  typedef std::pair<mem_type, mr_type> resource_type;

public:
  typedef std::pair<const value_type *, size_t> key_type;
  struct resource_entry {
    mem_type mem;
    size_t size;
    size_t key_size;
    uint32_t rkey;

  public:
    resource_entry() = default;
    resource_entry(resource_entry &&other) = default;
    resource_entry &operator=(resource_entry &&other) = default;
    resource_entry(mem_type &&mem, uint32_t rkey, size_t size, size_t key_size)
        : mem(std::move(mem)), size(size), key_size(key_size), rkey(rkey) {}
    value_type *key() const { return mem.get(); }
    void empty() {
      mem.reset();
      assert(mem.get() == nullptr);
      rkey = 0;
      size = 0;
      key_size = 0;
    }
  };

protected:
  key_entry *table;
  std::vector<resource_entry> shadow_table;
  size_t table_size;
  size_t used;
  const double growth_factor;

  bool index_valid(size_t index) const { return index < table_size; }
  bool index_invalid(size_t index) const { return !index_valid(index); }
  size_t invalid_index() const { return table_size + 1; }

public:
  server_dht(key_entry *table, size_t initial_size = 32,
             double growth_factor_ = 1.3)
      : table(table), shadow_table(initial_size), table_size(initial_size),
        used(0), growth_factor(growth_factor_) {
    for (size_t i = 0; i < table_size; i++) {
      log_info() << i << " " << (void *)shadow_table[i].mem.get();
      shadow_table[i].empty();
      table[i].empty();
    }
  }
  virtual ~server_dht() = default;
  server_dht(const server_dht &) = delete;
  server_dht(server_dht &&) = default;

  virtual Return_t add(resource_entry &&e) = 0;
  virtual Return_t remove(const key_type &key) = 0;
  virtual size_t contains(const key_type &key) = 0;
  void resize(key_entry *new_table, size_t size);
  
  size_t size() const { return table_size; }
  size_t next_size() const { return (size_t)(table_size * growth_factor); }
  double load_factor() const { return double(used) / table_size; }
};
}
