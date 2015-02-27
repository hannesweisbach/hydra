#pragma once

#include <utility>
#include <random>

#include "server_dht.h"
#include "util/Logger.h"

namespace hydra {

class cuckoo_server : public server_dht {
  struct resource_entry {
    mem_type mem;
    server_entry &entry;

  public:
    resource_entry(resource_entry &&other) = default;
    resource_entry &operator=(resource_entry &&other) {
      assert(other);
      assert(!*this);
      mem = std::move(other.mem);
      entry = std::move(other.entry);
      return *this;
    }
    
    explicit resource_entry(server_entry &entry) : entry(entry) {}
    
    const value_type *key() const { return mem.get(); }
    explicit operator bool() const noexcept {
      assert(bool(mem) == bool(entry.get()));
      return bool(mem);
    }

    void swap(std::tuple<mem_type, size_t, size_t, uint32_t> &entry_,
              uint32_t &seed_idx) {
      using std::swap;
      swap(mem, std::get<0>(entry_));

      entry([&](auto &&e) {
        using std::swap;
        /* swap(e.ptr.size, std::get<1>(entry_)); */
        size_t size = e.ptr.size;
        e.ptr = verifying_ptr<unsigned char>(mem.get(), std::get<1>(entry_));
        std::get<1>(entry_) = size;

        swap(e.key_size, std::get<2>(entry_));
        swap(e.rkey, std::get<3>(entry_));
        swap(e.hop, seed_idx);
      });
    }

    size_t hash() const noexcept { return entry.get().hop; }
    void set_hash(size_t hash) const noexcept {
      entry([hash = static_cast<uint32_t>(hash)](auto && e) { e.hop = hash; });
    }
    size_t size() const noexcept { return entry.get().ptr.size; }
    size_t key_size() const noexcept { return entry.get().key_size; }
    uint32_t rkey() const noexcept { return entry.get().rkey; }
    bool has_key(const key_type &other_key) const noexcept {
      return std::equal(key(), key() + key_size(), other_key.first,
                        other_key.first + other_key.second);
    }

    void empty() {
      entry([](auto &&entry) { entry.empty(); });
      mem.reset();
      assert(mem.get() == nullptr);
    }

    friend std::ostream &operator<<(
        std::ostream &s, const hydra::cuckoo_server::resource_entry &e) {
      s << "ptr: " << static_cast<void *>(e.mem.get()) << std::endl;
      s << "size: " << e.size() << std::endl;
      s << "key_size: " << e.key_size() << std::endl;
      s << "rkey: " << e.rkey();
      return s;
    }
  };

  std::mt19937_64 generator;
  std::uniform_int_distribution<uint64_t> distribution;
  std::array<uint64_t, 4> seeds;
  std::vector<resource_entry> shadow_table;
  size_t log_size = 0;
  bool rehashing = false;

  size_t index(const key_type &key, const uint64_t seed) const;
  void rehash();

public:
  cuckoo_server(LocalRDMAObj<hash_table_entry> *table, size_t initial_size) {
    log_info() << "sizeof(key_entry): " << sizeof(hash_table_entry);
    log_info() << "sizeof(resource_entry): " << sizeof(resource_entry);
    log_info() << "sizeof(mem_type): " << sizeof(mem_type);
    resize(table, initial_size);
    rehash();
    dump();
  }
  cuckoo_server(const cuckoo_server &) = delete;
  cuckoo_server(cuckoo_server &&) = default;
  Return_t add(std::tuple<mem_type, size_t, size_t, uint32_t> &e) override;
  Return_t remove(const key_type &key) override;
  void resize(LocalRDMAObj<hash_table_entry> *new_table, size_t size) override;
  size_t contains(const key_type &key) override;
  void dump(const size_t &, const size_t &) const;
  void dump() const override;
  void check_consistency() const override;
};
}
