#include <chrono>
#include <mutex>
#include "util/concurrent.h"
#include <algorithm>
#include <vector>

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
  asm volatile("rdtscp\n\t"
               "mov %%rdx, %0\n\t"
               "mov %%rax, %1\n\t"
               "cpuid\n\t"
               : "=r"(high), "=r"(low)::"%rax", "%rbx", "%rcx", "%rdx");
  return (high << 32) | low;
}

template <typename F>
static std::vector<uint64_t> measure(F &&f, size_t rounds) {
  std::vector<uint64_t> results;
  results.reserve(rounds);

  for (size_t i = 0; i < rounds; i++) {
    auto begin = start_benchmark();
    { f(); }
    auto end = end_benchmark();
    results.push_back(end - begin);
  }

  return results;
}

static std::vector<uint64_t> measure_mutex(size_t rounds) {
  std::mutex mutex;
  std::vector<uint64_t> results;
  results.reserve(rounds);

  for (size_t i = 0; i < rounds; i++) {
    auto begin = start_benchmark();
    { std::lock_guard<std::mutex> l(mutex); }
    auto end = end_benchmark();
    results.push_back(end - begin);
  }

  return results;
}

static std::vector<uint64_t> measure_spinlock(size_t rounds) {
  hydra::spinlock spinlock;
  std::vector<uint64_t> results;
  results.reserve(rounds);

  for (size_t i = 0; i < rounds; i++) {
    auto begin = start_benchmark();
    { std::lock_guard<hydra::spinlock> l(spinlock); }
    auto end = end_benchmark();
    results.push_back(end - begin);
  }

  return results;
}

int main() {
  constexpr size_t rounds = 1000 * 10;
  std::mutex mutex;
  hydra::spinlock spinlock;
  auto mutex_times =
      measure([&]() { std::lock_guard<std::mutex> l(mutex); }, rounds);
  auto spinlock_times =
      measure([&]() { std::lock_guard<hydra::spinlock> l(spinlock); }, rounds);

  auto mutex_times2 = measure_mutex(rounds);
  auto spinlock_times2 = measure_spinlock(rounds);

  std::cout << *std::min(std::begin(mutex_times), std::end(mutex_times))
            << std::endl;
  std::cout << *std::min(std::begin(mutex_times2), std::end(mutex_times2))
            << std::endl;
  std::cout << *std::min(std::begin(spinlock_times), std::end(spinlock_times))
            << std::endl;
  std::cout << *std::min(std::begin(spinlock_times2), std::end(spinlock_times2))
            << std::endl;
}
