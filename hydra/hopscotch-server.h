#include <utility>

#include "server_dht.h"
#include "util/Logger.h"

namespace hydra {

class hopscotch_server : public server_dht {
  struct resource_entry {
    mem_type mem;
    server_entry &entry;

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
      entry = std::move(other.entry);
      return *this;
    }
#endif
    explicit resource_entry(server_entry &entry) : entry(entry) {}
    const value_type *key() const { return mem.get(); }
    void set(resource_entry &home, const size_t &distance, mem_type ptr,
             size_t size, size_t key_size, uint32_t rkey) {
      mem = std::move(ptr);
      uint32_t hop = entry.get().hop;
      new (&entry) server_entry(mem.get(), size, key_size, rkey, hop);

      /* racy? */
      home.entry([&](auto &&entry) { entry.set_hop(distance); });
    }
    void into(resource_entry other, resource_entry &home,
              const size_t old_distance, const size_t new_distance) {
      assert(other);
      assert(!*this);

      /* copy and reset -- leaving the hop information intact.
       * other might be the same as home.
       */
      entry = other.entry;
      other.entry([&](auto &&entry) { entry.empty(); });

      /* update the hop information word */
      home.entry([&](auto &&entry) {
        entry.set_hop(new_distance);
        entry.clear_hop(old_distance);
      });

      /* move resources */
      mem = std::move(other.mem);
    }
    operator bool() const noexcept {
      assert(bool(mem) == bool(entry.get()));
      return bool(mem);
    }
    void empty(resource_entry &home, const size_t distance) {
      home.entry([&](auto &&entry) { entry.clear_hop(distance); });
      entry([](auto &&entry) { entry.empty(); });
      mem.reset();
      assert(mem.get() == nullptr);
    }
    size_t size() const noexcept { return entry.get().ptr.size; }
    size_t key_size() const noexcept { return entry.get().key_size; }
    uint32_t rkey() const noexcept { return entry.get().rkey; }
    bool has_hop(const size_t &idx) const noexcept {
      return entry.get().hop & (1 << idx);
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

    friend std::ostream &operator<<(
        std::ostream &s, const hydra::hopscotch_server::resource_entry &e) {
      s << "ptr: " << static_cast<void *>(e.mem.get()) << std::endl;
      s << "size: " << e.size() << std::endl;
      s << "key_size: " << e.key_size() << std::endl;
      s << "rkey: " << e.rkey();
      return s;
    }
  };

  const size_t hop_range;
  std::vector<resource_entry> shadow_table;

  size_t home_of(const hash_table_entry &e) const {
    return home_of(std::make_pair(e.key(), e.key_size));
  }

  template <typename Pred>
  size_t find_if(size_t begin, size_t end, Pred &&predicate) const {
    if (begin < end) {
      auto first = std::begin(shadow_table) + begin;
      auto last = std::begin(shadow_table) + end;
      auto result = std::find_if(first, last, predicate);
      if (result != last)
        return std::distance(std::begin(shadow_table), result);

      return invalid_index();
    } else {
      auto first = std::begin(shadow_table) + begin;
      auto last = std::end(shadow_table);
      auto result = std::find_if(first, last, predicate);
      if (result != last)
        return std::distance(std::begin(shadow_table), result);

      first = std::begin(shadow_table);
      last = std::begin(shadow_table) + begin;
      result = std::find_if(first, last, predicate);
      if (result != last)
        return std::distance(std::begin(shadow_table), result);

      return invalid_index();
    }
  }

  size_t home_of(const key_type &key) const;
  size_t next_free_index(size_t from) const;
  size_t next_movable(size_t to) const;
  void add(std::tuple<mem_type, size_t, size_t, uint32_t> &e, const size_t to,
           const size_t home);
  void move(size_t from, size_t to);
  size_t move_into(size_t to);

public:
  hopscotch_server(LocalRDMAObj<hash_table_entry> *table, size_t hop_range = 32,
                   size_t initial_size = 32)
      : hop_range(hop_range) {
    assert(
        ("Number of hops must be <= the number of bits in the type of the hop "
         "mask.",
         hop_range <=
             std::numeric_limits<decltype(hash_table_entry::hop)>::digits));
    log_info() << "sizeof(key_entry): " << sizeof(hash_table_entry);
    log_info() << "sizeof(resource_entry): " << sizeof(resource_entry);
    log_info() << "sizeof(mem_type): " << sizeof(mem_type);
    resize(table, initial_size);
  }
  hopscotch_server(const hopscotch_server &) = delete;
  hopscotch_server(hopscotch_server &&) = default;
  Return_t add(std::tuple<mem_type, size_t, size_t, uint32_t> &e) override;
  Return_t remove(const key_type &key) override;
  void resize(LocalRDMAObj<hash_table_entry> *new_table, size_t size) override;
  size_t contains(const key_type &key) override;
  size_t next_size() const override;
  void dump(const size_t &, const size_t &) const;
  void dump() const override;
  void check_consistency() const override;
};
}

