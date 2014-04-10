#include <thread>
#include <future>
#include <vector>
#include <cstdint>
#include <iostream>

#include "hydra/hash.h"

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

size_t hash_std(size_t bins) {
  std::hash<std::thread::id> hash;
  return hash(std::this_thread::get_id()) % bins;
}

size_t hash_city(size_t bins) {
  return hydra::hash64(std::this_thread::get_id()) % bins;
}

#define CITY 0
uint64_t benchmark(size_t count) {
  uint64_t start, end;
  start_benchmark();
  start_benchmark();
#if CITY
  hash_city(4);
  hash_city(4);
#else
  hash_std(4);
  hash_std(4);
#endif
  start_benchmark();

  start = start_benchmark();
  for (size_t i = 0; i < count; i++) {
#if CITY
    hash_city(4);
#else
    hash_std(4);
#endif
  }
  end = end_benchmark();

  return end - start;
}

void distribution(const size_t bins, const size_t n_threads) {
  std::vector<size_t> counts(bins, 0);
  std::vector<std::thread> threads;
  std::vector<std::future<size_t> > futures;
  for (size_t i = 0; i < n_threads; i++) {
    std::packaged_task<size_t(size_t)> task(hash_city);
    futures.push_back(task.get_future());
    threads.emplace_back(std::move(task), bins);
  }

  for (auto &thread : threads)
    thread.join();

  for (auto &future : futures)
    counts[future.get()]++;
  
  for (auto count : counts) {
    std::cout << count << std::endl;
  }
}

int main() {
#if 0
  size_t iterations = 10 * 1000;
  size_t benchmark_size = 10;
  std::vector<size_t> times(iterations);
  size_t min = std::numeric_limits<size_t>::max();
  for (size_t bsize = 0; bsize < benchmark_size; bsize++) {
    for (size_t i = 0; i < iterations; i++) {
      size_t b = benchmark(bsize);
      if (b < min)
        min = b;
      times[i] = b;
    }
    for (auto i : times) {
      std::cout << i << std::endl;
    }
  }

  // std::cout << "Min: " << min << std::endl;
#else
  distribution(4, 32 * 1000);
#endif
}
