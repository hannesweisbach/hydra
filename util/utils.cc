#include "concurrent.h"

namespace hydra {
spinlock::spinlock() noexcept : lock_(false) {}
spinlock::spinlock(spinlock &&other) noexcept : lock_(other.lock_.load()) {
//  lock_ = other.lock_.exchange(lock_);
}

spinlock &spinlock::operator=(spinlock &&other) noexcept {
  lock_.store(other.lock_.exchange(lock_.load()));

  return *this;
}

#if 0

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

#else

void spinlock::lock() {
  if (!lock_.exchange(true, std::memory_order_acquire))
    return;
  for (size_t i = 0; i < 1024; i++)
    if (!lock_.exchange(true, std::memory_order_acquire))
      return;
  while (lock_.exchange(true, std::memory_order_acquire))
    std::this_thread::yield();
}
void spinlock::unlock() { lock_.store(false, std::memory_order_release); }

#endif
}

