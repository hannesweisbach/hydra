#include "types.h"

namespace hydra {

std::ostream &operator<<(std::ostream &s, const hydra::node_id &id) {
  return ::operator<<(s << id.ip << ":" << id.port << " ", id.id);
}

std::ostream &operator<<(std::ostream &s, const hydra::interval &i) {
  return ::operator<<(::operator<<(s, i.start) << " - ", i.end);
}

std::ostream &operator<<(std::ostream &s, const hydra::routing_entry &e) {
  s << "routing_entry {" << std::endl;
  {
    indent_guard guard(s);
    s << indent << "node_id node = " << e.node << std::endl;
    s << indent << "interval interval = " << e.interval << std::endl;
  }
  return s << indent << "};";
}

std::ostream &operator<<(std::ostream &s, const hydra::routing_table &t) {
  s << "routing_table " << t.table.size() << std::endl;
  {
    indent_guard guard(s);
    size_t i = 0;
    for (auto &&e : t.table) {
      s << indent << " " << e << " " << std::setw(3) << i++ << std::endl;
    }
  }
  return s;
}
}

