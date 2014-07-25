#include <iomanip>

#include "hydra/network.h"

namespace hydra {
namespace overlay {
std::ostream &operator<<(std::ostream &s, const node_id &id) {
  return s << id.ip << ":" << id.port << " " << std::setw(6) << hex(id.id);
}

std::ostream &operator<<(std::ostream &s, const routing_entry &e) {
  return s << e.node << " " << std::setw(6) << e.start << " [" << e.node.id - e.start << "]";
}
}
}

