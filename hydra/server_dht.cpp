#include "server_dht.h"

namespace std {
std::ostream &operator<<(std::ostream &s,
                         const hydra::server_dht::resource_entry &e) {
  s << "ptr: " << static_cast<void *>(e.mem.get()) << std::endl;
  s << "size: " << e.size << std::endl;
  s << "key_size: " << e.key_size << std::endl;
  s << "rkey: " << e.rkey;
  return s;
}
}

void hydra::server_dht::resize(LocalRDMAObj<hash_table_entry> * new_table, size_t size) {
  size_t old_table_size = table_size;
  table_size = size;

  table = new_table;

  //TODO: initialize /w placement new
  for(size_t i = 0; i < size; i++)
    table[i]([](auto &&entry) { entry.empty(); });

  std::vector<resource_entry> old_shadow_table(table_size);
  std::swap(shadow_table, old_shadow_table);
  used = 0;

  for(auto&&entry : old_shadow_table) {
    if(entry.mem)
      add(std::move(entry));
  }
}

