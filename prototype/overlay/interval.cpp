#include <assert.h>

#include "hydra/types.h"

int main() {
  
  using namespace hydra::literals;

  const hydra::keyspace_t k = 4;
  const auto inc = k + 1;
  const auto dec = k - 1;
  const auto inc2 = k + 2;
  const auto dec2 = k + 2;

  assert(k.in(k, k));
  assert(!inc.in(k, k));
  assert(!dec.in(k, k));
  auto foo = k + std::numeric_limits<hydra::keyspace_t::value_type>::max() / 2;
  assert(!foo.in(k, k));

  assert(k.in(dec, inc));
  assert(dec.in(dec, inc));
  assert(inc.in(dec, inc));
  assert(!dec2.in(dec, inc));
  assert(!inc2.in(dec, inc));

  assert(!k.in(inc, dec));
  assert(dec.in(inc, dec));
  assert(inc.in(inc, dec));
  assert(dec2.in(inc, dec));
  assert(inc2.in(inc, dec));

  assert((15_ID).in(10_ID, 15_ID));

}
