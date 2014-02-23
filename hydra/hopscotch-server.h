#include <utility>

#include "server_dht.h"

namespace hydra {

class hopscotch_server : public server_dht {
  const size_t hop_range;

  size_t home_of(const key_entry &e) const {
    return home_of(std::make_pair(e.key(), e.key_size));
  }
  size_t home_of(const key_type &key) const;
  size_t next_free_index(size_t from) const;
  size_t next_movable(size_t to) const;
  void add(resource_entry &&e, const size_t to, const size_t home);
  void move(size_t from, size_t to);
  size_t move_into(size_t to);

public:
  hopscotch_server(key_entry * table, size_t hop_range = 32, size_t initial_size = 32)
      : server_dht(table, initial_size), hop_range(hop_range) {}
  hopscotch_server(const hopscotch_server &) = delete;
  hopscotch_server(hopscotch_server &&) = default;
  Return_t add(resource_entry &&e) override;
  Return_t remove(const key_type& key) override;
  size_t contains(const key_type &key) override;
  void dump() const;
  void check_consistency() const;
};
}

