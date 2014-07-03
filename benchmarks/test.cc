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

#if 0
std::unique_ptr<void> foo() {
  std::unique_ptr<void, void(*)(void*)> p1(malloc(10), [](void *p) {
    std::cout << "Deallocating p1" << std::endl;
    free(p);
  });
  auto lambda = [ =, p_ = std::move(p1) ](void * p) mutable {
    std::cout << "Deleter" << std::endl;
    free(p);
  };
  
  return std::unique_ptr<void, decltype(lambda)>(malloc(10), std::move(lambda));
}
#endif
int main(int argc, char * const argv[]) {
  std::cout << "Size of std::atomic_flag: " << sizeof(std::atomic_flag) << std::endl;
  std::cout << "Size " << sizeof(RdmaHeap<ibv_access::MSG>) << " "
            << sizeof(hydra::PerThreadHeap<RdmaHeap<ibv_access::MSG> >) << " "
            << sizeof(hydra::LockedHeap<RdmaHeap<ibv_access::MSG> >)
            << std::endl;
  std::unique_ptr<void, std::function<void(void *)> > p1(malloc(10),
                                                         [](void *p) {
    std::cout << "Dealloc" << std::endl;
    ::free(p);
  });

  std::cout << p1.get() << std::endl;

  std::unique_ptr<void, std::function<void(void*)>> p2;
  
  std::cout << p2.get() << std::endl;
  
  p2 = (std::move(p1));

  std::cout << p1.get() << " " << p2.get() << std::endl;

  size_t to = 0;
  size_t home = 12;
  size_t size = 13;

  std::cout << std::hex << (to - home + size) << std::endl;
  std::cout << (to - home) % size << std::endl;;

  using namespace hydra;
  RDMAServerSocket socket("10.0.0.1", "8042");
  auto size2Class = [](size_t size) {
    size = std::max(size, 0x80UL);
    size_t class_ =
        hydra::util::log2(size) - hydra::util::static_log2<0x80>::value;
    return class_;
  };
  PerThreadHeap<LockedHeap<SegregatedFitsHeap<
      FreeListHeap<ZoneHeap<RdmaHeap<ibv_access::READ>, 1024 * 1024 * 32> >,
      ZoneHeap<RdmaHeap<ibv_access::READ>, 256> > > > heap(16U, 5U, size2Class,
                                                           socket);

  log_info() << "Measurement resolution: "
             << std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::duration(1)).count()
             << " ns";
  /* pre - allocate */
  auto start = std::chrono::high_resolution_clock::now();
  {
    std::vector<decltype(heap.malloc<char>(16))> ptrs(1024*1024);
    for (size_t i = 0; i < 1024 * 1024; i++) {
      ptrs.push_back(heap.malloc<char>(16));
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "Measured time: " << dur.count() << " ns";

  start = std::chrono::high_resolution_clock::now();
  std::vector<decltype(heap.malloc<char>(16))> ptrs(1024 * 1024);
  for (size_t i = 0; i < 1024 * 1024; i++) {
    ptrs.push_back(heap.malloc<char>(16));
  }
  end = std::chrono::high_resolution_clock::now();
  dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "Measured time: " << dur.count() << " ns";
  
  start = std::chrono::high_resolution_clock::now();
  std::vector<decltype(heap.malloc<char>(16))> ptrs_(1024*1024);
  for (size_t i = 0; i < 1024 * 1024; i++) {
    ptrs_.push_back(decltype(heap.malloc<char>(16))());
  }
  end = std::chrono::high_resolution_clock::now();
  dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  log_info() << "Measured time: " << dur.count() << " ns";
  
  if(argc < 2)
    return EXIT_FAILURE;
  size_t v;
  sscanf(argv[1], "%zd", &v);
  printf("log of %zd is %zd\n", v, hydra::util::log2_(v));
  printf("log of %zd is %zd\n", v, hydra::util::log2(v));

  /*
  RDMAAddrinfo ai("", "8042");

  for(const rdma_addrinfo& i : ai) {
    std::cout << i.ai_src_len << std::endl;

  }
*/
}
