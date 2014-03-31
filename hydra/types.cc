#include "types.h"

namespace hydra {

std::ostream &operator<<(std::ostream &s, const hydra::node_id &id) {
  return s << id.ip << ":" << id.port << " " << id.id;
}

std::ostream &operator<<(std::ostream &s, const hydra::interval &i) {
  return s << "[" << i.start << " - " << i.end << ")";
}

std::ostream &operator<<(std::ostream &s, const hydra::routing_entry &e) {
  return s << e.node << " " << e.interval;
}

std::ostream &operator<<(std::ostream &s, const hydra::routing_table &t) {
  s << "routing_table " << t.table.size() << std::endl;
  {
    indent_guard guard(s);
    s << indent << "pred: " << t.predecessor() << std::endl;
    s << indent << "self: " << t.self() << std::endl;
    size_t i = 1;
    for (auto &&e : t) {
      s << indent << "[" << std::setw(3) << i++ << "] " << e
        << " " << std::endl;
    }
  }
  return s;
}
}

