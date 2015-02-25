#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <functional>
#include <vector>
#include <ostream>

#include "types.h"
#include "util/concurrent.h"

#define PER_ENTRY_LOCKS 0

namespace hydra {
class server_dht {
protected:
  typedef unsigned char value_type;
#if 1
  typedef std::unique_ptr<value_type, std::function<void(value_type *)> >
  mem_type;
#else
  using mem_type = std::shared_ptr<value_type>;
#endif
  typedef std::unique_ptr<ibv_mr, std::function<void(ibv_mr *)> > mr_type;
  typedef std::pair<mem_type, mr_type> resource_type;

  LocalRDMAObj<hash_table_entry> *table = nullptr;
  size_t used_ = 0;
  size_t table_size = 0;
  size_t rehash_count = 0;
  const double growth_factor;

  bool index_valid(size_t index) const { return index < table_size; }
  bool index_invalid(size_t index) const { return !index_valid(index); }
  size_t invalid_index() const { return table_size + 1; }

public:
  typedef std::pair<const value_type *, size_t> key_type;

  server_dht(double growth_factor_ = 1.3) : growth_factor(growth_factor_) {}
  virtual ~server_dht() = default;
  server_dht(const server_dht &) = delete;
  server_dht(server_dht &&) = default;

  virtual Return_t add(std::tuple<mem_type, size_t, size_t, uint32_t>& e) = 0;
  virtual Return_t remove(const key_type &key) = 0;
  virtual size_t contains(const key_type &key) = 0;
  virtual void resize(LocalRDMAObj<hash_table_entry> *new_table,
                      size_t size) = 0;

  virtual void check_consistency() const = 0;
  virtual void dump() const = 0;

  size_t size() const noexcept { return table_size; }
  size_t used() const noexcept { return used_; }
  virtual size_t next_size() const {
    return (size_t)(table_size * growth_factor);
  }
  double load_factor() const noexcept { return double(used_) / table_size; }
  size_t rehashes() const noexcept { return rehash_count; }
};
}

