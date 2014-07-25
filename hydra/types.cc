#include <iomanip>
#include "types.h"

namespace hydra {

template <>
std::ostream &operator<<(std::ostream &s, const hex_wrapper<uint8_t> hex) {
  if (hex.v) {
    auto fmtflags = s.flags();
    auto old_fill = s.fill('0');
    s << "0x" << std::setw(2) << std::hex << std::noshowbase
      << static_cast<unsigned>(hex.v);
    s.flags(fmtflags);
    s.fill(old_fill);
  } else {
    s << "0x00";
  }
  return s;
}

std::ostream &operator<<(std::ostream &s, const keyspace_t &rhs) {
  return s << hydra::hex(rhs.value__);
}
}

std::ostream &operator<<(std::ostream &s, const mr &mr) {
  s << "mr {" << std::endl;
  {
    indent_guard guard(s);
    s << indent << "uint64_t addr = " << std::hex << std::showbase
      << std::setfill('0') << std::setw(12) << mr.addr << std::dec << std::endl;
    s << indent << "uint32_t size = " << mr.size << std::endl;
    s << indent << "uint32_t rkey = " << mr.rkey << std::endl;
  }
  s << indent << "};";
  return s;
}

