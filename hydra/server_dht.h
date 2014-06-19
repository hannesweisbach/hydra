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
  typedef std::unique_ptr<value_type, std::function<void(value_type *)> >
  mem_type;
  typedef std::unique_ptr<ibv_mr, std::function<void(ibv_mr *)> > mr_type;
  typedef std::pair<mem_type, mr_type> resource_type;

public:
  typedef std::pair<const value_type *, size_t> key_type;
  struct resource_entry {
    mem_type mem;
    server_entry &rdma_entry;

#if PER_ENTRY_LOCKS
    mutable hydra::spinlock lock_;
#endif

  public:
    // resource_entry() = default;
    resource_entry(resource_entry &&other) = default;
#if PER_ENTRY_LOCKS
    resource_entry &operator=(resource_entry &&other) {
      /* if other lock is taken, take ours */

      if (!other.lock_.try_lock())
        lock_.try_lock();
      mem = std::move(other.mem);
      other.lock_.unlock();
      return *this;
    }
#else
    resource_entry &operator=(resource_entry &&other) {
      assert(other);
      assert(!*this);
      mem = std::move(other.mem);
      rdma_entry = std::move(other.rdma_entry);
      return *this;
    }
#endif
    explicit resource_entry(server_entry &rdma_entry)
        : rdma_entry(rdma_entry) {}
    const value_type *key() const { return mem.get(); }
    void set(mem_type ptr, size_t size, size_t key_size, uint32_t rkey) {
      mem = std::move(ptr);
      uint32_t hop = rdma_entry.get().hop;
      new (&rdma_entry) server_entry(mem.get(), size, key_size, rkey, hop);
    }
    operator bool() const noexcept {
      assert(bool(mem) == bool(rdma_entry.get()));
      return bool(mem);
    }
    void empty() {
      mem.reset();
      rdma_entry = server_entry();
      assert(mem.get() == nullptr);
    }
    size_t size() const noexcept { return rdma_entry.get().ptr.size; }
    size_t key_size() const noexcept { return rdma_entry.get().key_size; }
    uint32_t rkey() const noexcept { return rdma_entry.get().rkey; }
    bool has_hop(const size_t &idx) const noexcept {
      return rdma_entry.get().hop & (1 << idx);
    }
    void clear_hop(const size_t &idx) noexcept {
      rdma_entry([&](auto &&entry) { entry.clear_hop(idx); });
    }
    bool has_key(const key_type &other_key) const noexcept {
      return (key_size() == other_key.second) &&
             std::equal(key(), key() + key_size(), other_key.first);
    }
    void lock() const noexcept {
#if PER_ENTRY_LOCKS
      lock_.lock();
#endif
    }
    void unlock() const noexcept {
#if PER_ENTRY_LOCKS
      lock_.unlock();
#endif
    }
    void debug() noexcept {
#if PER_ENTRY_LOCKS
      lock_.__debug();
#endif
    }
  };

protected:
  LocalRDMAObj<hash_table_entry> *table = nullptr;
  std::vector<resource_entry> shadow_table;
  size_t table_size = 0;
  size_t used = 0;
  const double growth_factor;

  bool index_valid(size_t index) const { return index < table_size; }
  bool index_invalid(size_t index) const { return !index_valid(index); }
  size_t invalid_index() const { return table_size + 1; }

public:
  server_dht(LocalRDMAObj<hash_table_entry> *table, size_t initial_size = 32,
             double growth_factor_ = 1.3)
      : growth_factor(growth_factor_) {
    resize(table, initial_size);
  }
  virtual ~server_dht() = default;
  server_dht(const server_dht &) = delete;
  server_dht(server_dht &&) = default;

  virtual Return_t add(std::tuple<mem_type, size_t, size_t, uint32_t>& e) = 0;
  virtual Return_t remove(const key_type &key) = 0;
  virtual size_t contains(const key_type &key) = 0;
  void resize(LocalRDMAObj<hash_table_entry> *new_table, size_t size);

  size_t size() const { return table_size; }
  virtual size_t next_size() const {
    return (size_t)(table_size * growth_factor);
  }
  double load_factor() const { return double(used) / table_size; }
};
}

namespace std {
std::ostream &operator<<(std::ostream &s,
                         const hydra::server_dht::resource_entry &e);
}
