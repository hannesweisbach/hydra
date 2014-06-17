#include "server_dht.h"

namespace std {
std::ostream &operator<<(std::ostream &s,
                         const hydra::server_dht::resource_entry &e) {
  s << "ptr: " << static_cast<void *>(e.mem.get()) << std::endl;
  s << "size: " << e.size() << std::endl;
  s << "key_size: " << e.key_size() << std::endl;
  s << "rkey: " << e.rkey();
  return s;
}
}

void hydra::server_dht::resize(LocalRDMAObj<hash_table_entry> *new_table,
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
  used = 0;

  for (auto &&entry : tmp_shadow_table) {
    if (entry)
      add(std::make_tuple(std::move(entry.mem), entry.size(), entry.key_size(),
                          entry.rkey()));
  }
}

