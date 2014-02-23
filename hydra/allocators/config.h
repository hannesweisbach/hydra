#pragma once

#include <cstddef>
#include <assert.h>

#include "util/utils.h"

namespace hydra {
class AllocatorConfig {
public:
  static const size_t Alignment = 128;
};

template <size_t Alignment> size_t align(size_t size) {
  assert(is_power_of_two<Alignment>::value);
  return ((size + (Alignment - 1)) & ~(Alignment - 1));
}
}
