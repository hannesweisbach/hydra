#include "concurrent.h"

namespace hydra {
spinlock::spinlock() noexcept : lock_(false) {}

void spinlock::lock() noexcept {
  if (!lock_.test_and_set(std::memory_order_acquire))
    return;
  for (size_t i = 0; i < 1024; i++)
    if (!lock_.test_and_set(std::memory_order_acquire))
      return;
  while (lock_.test_and_set(std::memory_order_acquire))
    std::this_thread::yield();
}
void spinlock::unlock() noexcept { lock_.clear(std::memory_order_release); }
}

