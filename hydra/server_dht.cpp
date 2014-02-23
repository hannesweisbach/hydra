#include "server_dht.h"

void hydra::server_dht::resize(key_entry * new_table, size_t size) {
  size_t old_table_size = table_size;
  table_size = size;

  table = new_table;

  for(size_t i = 0; i < size; i++)
    table[i].empty();
  
  std::vector<resource_entry> old_shadow_table(table_size);
  std::swap(shadow_table, old_shadow_table);

  for(size_t i = 0; i < old_table_size; i++) {
    add(std::move(old_shadow_table[i]));
  }
}

