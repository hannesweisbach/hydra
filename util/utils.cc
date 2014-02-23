#include "concurrent.h"

namespace hydra {
spinlock::spinlock() noexcept : lock_(ATOMIC_FLAG_INIT) {}
void spinlock::lock() {
  if (!lock_.test_and_set(std::memory_order_acquire))
    return;
  for (size_t i = 0; i < 1024; i++)
    if (!lock_.test_and_set(std::memory_order_acquire))
      return;
  while (lock_.test_and_set(std::memory_order_acquire))
    std::this_thread::yield();
}
void spinlock::unlock() { lock_.clear(std::memory_order_release); }
}

