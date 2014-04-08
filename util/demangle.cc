#include <cxxabi.h>
#include <memory>

#include "util/demangle.h"

std::string hydra::util::demangle(const char *mangled_name) {
  int status;
  std::unique_ptr<char, void (*)(void *)> demangled_name(
      abi::__cxa_demangle(mangled_name, nullptr, 0, &status), ::free);
  return (status == 0) ? demangled_name.get() : mangled_name;
}
