#include <utility>

#include "server_dht.h"
#include "util/Logger.h"

namespace hydra {

class hopscotch_server : public server_dht {
  const size_t hop_range;
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
      : server_dht(table, initial_size), hop_range(hop_range) {
    assert(
        ("Number of hops must be <= the number of bits in the type of the hop "
         "mask.",
         hop_range <=
             std::numeric_limits<decltype(hash_table_entry::hop)>::digits));
    log_info() << "sizeof(key_entry): " << sizeof(hash_table_entry);
    log_info() << "sizeof(resource_entry): " << sizeof(resource_entry);
    log_info() << "sizeof(mem_type): " << sizeof(mem_type);
  }
  hopscotch_server(const hopscotch_server &) = delete;
  hopscotch_server(hopscotch_server &&) = default;
  Return_t add(std::tuple<mem_type, size_t, size_t, uint32_t> &e) override;
  Return_t remove(const key_type& key) override;
  size_t contains(const key_type &key) override;
  size_t next_size() const override;
  void dump(const size_t &, const size_t &) const;
  void dump() const;
  void check_consistency() const;
};
}

