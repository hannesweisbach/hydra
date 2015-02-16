#include <iostream>
#include <limits>
#include <chrono>

#include "rdma/RDMAServerSocket.h"
#include "hydra/allocators/allocators.h"

#define ALLOC_ONLY 1

void bench_map(RDMAServerSocket &socket, size_t measurements, size_t max_size, size_t step = 8) {
  for (size_t size = step; size < max_size; size += step) {
    size_t min = std::numeric_limits<size_t>::max();
    size_t max = std::numeric_limits<size_t>::min();
    for (size_t measurement = 0; measurement < measurements; measurement++) {

      std::vector<char> dummy_data(size);

      auto start = std::chrono::high_resolution_clock::now();

#if ALLOC_ONLY
      auto mr = socket.register_memory(
          ibv_access::LOCAL_WRITE | ibv_access::REMOTE_READ, dummy_data);
#else
      {
        auto mr = socket.register_memory(
            ibv_access::LOCAL_WRITE | ibv_access::REMOTE_READ, dummy_data);
      }
#endif

      auto end = std::chrono::high_resolution_clock::now();
      size_t current = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end - start).count();

      if (current < min)
        min = current;
      if(current > max)
        max = current;
    }
    std::cout << "Size: " << std::setw(10) << size << ", Min: " << std::setw(6)
              << min << "ns, Max: " << std::setw(6) << max << "ns" << std::endl;
  }
}

void bench_small_alloc(RDMAServerSocket &socket, size_t measurements, size_t max_size, size_t step = 8) {
  default_heap_t heap(48U, default_size_classes, socket);

  for (size_t size = step; size < max_size; size += step) {
    size_t min = std::numeric_limits<size_t>::max();
    size_t max = std::numeric_limits<size_t>::min();
    for (size_t measurement = 0; measurement < measurements; measurement++) {

      char ptr[size];

      auto start = std::chrono::high_resolution_clock::now();

#if ALLOC_ONLY
      auto mem = heap.malloc<char>(size);
      memcpy(mem.first.get(), ptr, size);
#else
      {
        auto mem = heap.malloc<char>(size);
        memcpy(mem.first.get(), ptr, size);
      }
#endif
      auto end = std::chrono::high_resolution_clock::now();
      size_t current = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end - start).count();

      if (current < min)
        min = current;
      if(current > max)
        max = current;
    }
    std::cout << "Size: " << std::setw(10) << size << ", Min: " << std::setw(6)
              << min << "ns, Max: " << std::setw(6) << max << "ns" << std::endl;
  }
}

int main() {
  RDMAServerSocket socket("10.1", "8042");

  //bench_map(socket, 10000, 64 * 1024, 1024);
  bench_small_alloc(socket, 10000, 64 * 1024, 1024);
}

