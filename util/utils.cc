#include "concurrent.h"

#define YIELD 0

namespace hydra {
spinlock::spinlock() noexcept : locks(0), spins(0), yields(0), lock_(false) {}
spinlock::spinlock(const std::string &name) noexcept : name(name),
                                                       locks(0),
                                                       spins(0),
                                                       yields(0),
                                                       lock_(false) {}
void spinlock::lock() const noexcept {
  locks++;
  if (!lock_.test_and_set(std::memory_order_acquire)) {
    return;
  }

#if YIELD
  for (size_t i = 0; i < 1024; i++) {
#else
  for (;;) {
#endif
    if (!lock_.test_and_set(std::memory_order_acquire)) {
      return;
    }
    asm volatile("pause":::);
    spins++;
  }
#if YIELD
  while (lock_.test_and_set(std::memory_order_acquire)) {
    std::this_thread::yield();
    yields++;
  }
#endif
}
bool spinlock::try_lock() const noexcept {
  return !lock_.test_and_set(std::memory_order_acquire);
}
void spinlock::unlock() const noexcept { lock_.clear(std::memory_order_release); }

void spinlock::__debug() noexcept {
  if (name.empty())
    log_debug() << static_cast<void *>(this) << " " << locks.load() << " "
                << spins.load() << " " << yields.load();
  else
    log_debug() << name << " " << locks.load() << " " << spins.load() << " "
                << yields.load();
}
}

