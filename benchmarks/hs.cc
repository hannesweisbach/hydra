#include <vector>
#include <tuple>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <chrono>
#include <thread>

#include "hydra/server_dht.h"
#include "hydra/hopscotch-server.h"

#define SPIN_LOCK 1

#if PER_ENTRY_LOCKS
using hash_table_t = hydra::hopscotch_server;
#elif SPIN_LOCK
#include "util/concurrent.h"
using hash_table_t = monitor<hydra::hopscotch_server, hydra::spinlock>;
#else
using hash_table_t = monitor<hydra::hopscotch_server>;
#endif

void add(hash_table_t &dht, const size_t &size, const size_t &elems) {
  using namespace std::literals::chrono_literals;
    
  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');
  
  size_t elem = 0;
  auto generate_request = [&]() {
    std::unique_ptr<unsigned char, std::function<void(unsigned char *)> > ptr(
        reinterpret_cast<unsigned char *>(::malloc(size)), ::free);

    std::ostringstream ss;
    ss << std::setw(4) << elem;

#if 0
    elem = (elem + 1) % 255;
#else
    elem++;
#endif

    const size_t key_size = ss.str().size();
    memcpy(ptr.get(), ss.str().c_str(), key_size);
#if 1
    std::generate_n(ptr.get() + key_size, size - key_size, std::bind(distribution, generator));
#else
    /* simulate transfer delay */
    std::this_thread::sleep_for(10us);
#endif
    const uint32_t rkey = 1;
    return std::make_tuple(std::move(ptr), size, key_size, rkey);
  };

  auto gen_time = 0ns;
  auto add_time = 0ns;
  size_t added;
  std::cout << "Running" << std::endl;
  for (added = 0; added < elems; added++) {
    auto start = std::chrono::high_resolution_clock::now();
    auto request = generate_request();
    auto end = std::chrono::high_resolution_clock::now();

    gen_time +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    start = std::chrono::high_resolution_clock::now();
    auto ret = dht([request = std::move(request)](auto && dht) mutable {
      //dht.check_consistency();
      auto ret = dht.add(request);
      //dht.check_consistency();
      return ret;
    });
    end = std::chrono::high_resolution_clock::now();

    add_time +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    if (ret == hydra::NEED_RESIZE)
      break;
  }

  if(added != elems)
    std::cout << "Aborted after " << added << std::endl;

  std::cout << "Took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(gen_time)
                   .count() << "ms to generate data" << std::endl;
  std::cout << "Took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(add_time)
                   .count() << "ms to add entries" << std::endl;

  std::cout << added / std::chrono::duration_cast<std::chrono::milliseconds>(
                           add_time + gen_time).count() << " kOps/s (gen + add)"
            << std::endl;
  std::cout << added / std::chrono::duration_cast<std::chrono::milliseconds>(
                           add_time).count() << " kOps/s (add)" << std::endl;
}


int main() {
  const size_t hop_range = 32;
  const size_t table_size = 20000;
  const size_t elems = 200000;
  const size_t size = 64;

  const size_t min_threads = 1;
  const size_t max_threads = 2;

  static_assert(min_threads < max_threads, "The minimum number of threads must "
                                           "be strictly smaller than the "
                                           "maximum number");

  for (size_t cur_threads = min_threads; cur_threads < max_threads;
       cur_threads++) {
    std::cout << "Running with " << cur_threads << " threads.";
    std::cout.flush();

    std::vector<LocalRDMAObj<hydra::hash_table_entry> > table(table_size);
    hash_table_t dht(table.data(), hop_range, table_size);

    std::vector<std::thread> threads;
    threads.reserve(cur_threads);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::generate_n(std::back_inserter(threads), cur_threads, [&]() {
      return std::thread(add, std::ref(dht), std::cref(size), std::cref(elems));
    });

    for (auto &&thread : threads) {
      thread.join();
    }
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration_cast<std::chrono::seconds>(end - start)
                     .count() << "s" << std::endl;
    std::cout << (elems * cur_threads) /
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         end - start).count() << " kOps/s (gen + add)"
              << std::endl;
  }
}

