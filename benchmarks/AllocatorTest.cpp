#include <iostream>
#include <cstdio>
#include <memory>
#include <algorithm>
#include <chrono>

#include "rdma/RDMAServerSocket.h"
#include "hydra/node.h"
#include "RDMAWrapper.hpp"
#include "util/utils.h"

#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"
#include "allocators/LockedHeap.h"
#include "allocators/PerThreadAllocator.h"
#include "RDMAAllocator.h"

using namespace hydra;
using Heap_t = PerThreadHeap<LockedHeap<SegregatedFitsHeap<
    FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 1024 * 1024> >,
    ZoneHeap<RdmaHeap<ibv_access::READ>, 256> > > >;

void time_allocation(Heap_t& heap, size_t count = 1024 * 1024, size_t size = 16) {
  std::hash<std::thread::id> hash;
  log_info() << "Starting thread " << std::hex << std::showbase
             << std::this_thread::get_id() << " " << std::showbase
             << hash(std::this_thread::get_id()) << " " << 
             hydra::hash64(std::this_thread::get_id()) << std::dec;
  /* pre - allocate */
  auto start = std::chrono::high_resolution_clock::now();
  {
    std::vector<decltype(heap.malloc<char>(size))> ptrs(count);
    for (size_t i = 0; i < count; i++) {
      ptrs.push_back(heap.malloc<char>(size));
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "Warm up (de-)allocation + storage: " << dur.count() / count
             << " ns";

  start = std::chrono::high_resolution_clock::now();
  std::vector<decltype(heap.malloc<char>(size))> ptrs(count);
  for (size_t i = 0; i < count; i++) {
    ptrs.push_back(heap.malloc<char>(size));
  }
  end = std::chrono::high_resolution_clock::now();
  auto hot_stor = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "Hot allocation + storage: " << hot_stor.count() / count << " ns";

  std::vector<decltype(heap.malloc<char>(size))> ptrs_(count);
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < count; i++) {
    ptrs_.push_back(decltype(heap.malloc<char>(size))());
  }
  end = std::chrono::high_resolution_clock::now();
  auto stor = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "storage " << stor.count()/count << " ns";
  if (hot_stor > stor) {
    log_info() << "Hot allocation (calculated): " << (hot_stor - stor).count() /
                                                         count << " ns";
  } else {
    log_info() << "Hot allocation not calculable.";
  }
} 

void multi_thread_alloc(Heap_t &heap, size_t num_threads = 4,
                        size_t count = 1024 * 1024, size_t size = 16) {
  log_info() << "Running multithreaded allocation test with " << num_threads  << " threads.";
#if 0
  std::vector<std::future<void> > futs;
  for (size_t i = 0; i < num_threads; i++)
    futs.push_back(hydra::async([&]() mutable { time_allocation(heap); }));
  for (auto &fut : futs)
    fut.get();
#else
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads; i++)
    threads.emplace_back([&]() mutable { time_allocation(heap, count, size); });
  for (auto &thread : threads)
    thread.join();
#endif
}

int main(int argc, char *const argv[]) {
  std::cout << "Size of std::atomic_flag: " << sizeof(std::atomic_flag)
            << std::endl;
  std::cout << "Size " << sizeof(RdmaHeap<ibv_access::MSG>) << " "
            << sizeof(PerThreadHeap<RdmaHeap<ibv_access::MSG> >) << " "
            << sizeof(hydra::LockedHeap<RdmaHeap<ibv_access::MSG> >)
            << std::endl;

  RDMAServerSocket socket("10.0.0.1", "8042");
  auto size2Class = [](size_t size) {
    size = std::max(size, 0x10UL);
    size_t class_ =
        hydra::util::log2(size) - hydra::util::static_log2<0x10>::value;
    return class_;
  };
  Heap_t heap(4U, 1U, size2Class, socket);

  log_info() << "Measurement resolution: "
             << std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::duration(1)).count()
             << " ns";

  //time_allocation(heap);
  multi_thread_alloc(heap, 4, 1024*512);
}
