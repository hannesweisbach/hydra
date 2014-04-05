#include "hydra/chord.h"
#include "hydra/passive.h"

namespace hydra {
namespace chord {
static routing_table find_table(routing_table table, const keyspace_t &id) {
  while (!id.in(table.self().node.id + 1, table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    hydra::passive node(re.ip, re.port);
    table = node.table();
  }
  return table;
}

node_id predecessor(const routing_table &start, const keyspace_t &id) {
  return find_table(start, id).self().node;
}

node_id successor(const routing_table &start, const keyspace_t &id) {
  return find_table(start, id).successor().node;
}
}
}
