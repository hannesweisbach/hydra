#include <chrono>
#include <mutex>
#include <algorithm>
#include <vector>

#include "util/concurrent.h"

static uint64_t start_benchmark() {
  uint64_t high, low;
#ifdef __clang__
  asm volatile("cpuid\n\t"
               "rdtsc\n\t"
               : "=d"(high), "=a"(low)::"%rax", "%rbx", "%rcx", "%rdx");
#else
  asm volatile("cpuid\n\t"
               "rdtsc\n\t"
               "mov %%rdx, %0\n\t"
               "mov %%rax, %1\n\t"
               : "=r"(high), "=r"(low)::"%rax", "%rbx", "%rcx", "%rdx");
#endif
  return (high << 32) | low;
}

static uint64_t end_benchmark() {
  uint64_t high, low;
#if 1
  asm volatile("rdtscp\n\t"
#else
  asm volatile("rdtsc\n\t"
#endif
               "mov %%rdx, %0\n\t"
               "mov %%rax, %1\n\t"
               "cpuid\n\t"
               : "=r"(high), "=r"(low)::"%rax", "%rbx", "%rcx", "%rdx");
  return (high << 32) | low;
}

template <typename Lock>
static std::vector<uint64_t> measure(Lock &&lock, size_t rounds) {
  std::vector<uint64_t> results;
  results.reserve(rounds);

  for (size_t i = 0; i < rounds; i++) {
#if 0
    { std::unique_lock<std::remove_reference_t<Lock>> l(lock); }
#else
    { std::lock_guard<Lock> l(lock); }
#endif
  }

  for (size_t i = 0; i < rounds; i++) {
    auto begin = start_benchmark();
#if 0
    { std::unique_lock<std::remove_reference_t<Lock>> l(lock); }
#else
    { std::lock_guard<Lock> l(lock); }
#endif
    auto end = end_benchmark();
    results.push_back(end - begin);
  }

  return results;
}

int main() {
  constexpr size_t rounds = 1000 * 10;
  std::mutex mutex;
  hydra::spinlock spinlock;
  std::shared_timed_mutex rw;

  auto mutex_times = measure(mutex, rounds);
  auto spinlock_times = measure(spinlock, rounds);
  auto rw_times = measure(rw, rounds);

  std::cout << *std::min(std::begin(mutex_times), std::end(mutex_times))
            << std::endl;
  std::cout << *std::min(std::begin(spinlock_times), std::end(spinlock_times))
            << std::endl;
  std::cout << *std::min(std::begin(rw_times), std::end(rw_times)) << std::endl;
}
