#pragma once

#include "hydra/types.h"

namespace hydra {
namespace chord {
node_id predecessor(const routing_table &start, const keyspace_t &id);
node_id successor(const routing_table &start, const keyspace_t &id);
}
}

