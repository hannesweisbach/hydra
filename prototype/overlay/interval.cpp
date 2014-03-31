#include <assert.h>

#include "hydra/types.h"

int main() {
  
  const hydra::keyspace_t k = 4;
  hydra::interval same({k, k});
  hydra::interval inc({k-1, k+1});
  hydra::interval dec({k+1, k-1});

  assert(same.contains(k));
  assert(!same.contains(k+1));
  assert(!same.contains(k-1));
  assert(!same.contains(-k));

  assert(inc.contains(k));
  assert(inc.contains(k-1));
  assert(inc.contains(k+1));
  assert(!inc.contains(k-2));
  assert(!inc.contains(k+2));
  
  assert(!dec.contains(k));
  assert(dec.contains(k-1));
  assert(dec.contains(k+1));
  assert(dec.contains(k-2));
  assert(dec.contains(k+2));

  assert(hydra::interval({10, 15}).contains(15));


}
