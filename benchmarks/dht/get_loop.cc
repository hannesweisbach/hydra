#include <random>
#include <sstream>
#include <atomic>
#include <thread>
#include <string>
#include <vector>

#include "hydra/passive.h"

static void get_keys(const std::string &host, const std::string &port,
                     const size_t max_keys, std::atomic_bool &run,
                     std::atomic<uint64_t> &found,
                     std::atomic<uint64_t> &notfound) {
  using key_t = std::vector<unsigned char>;
  hydra::passive socket(host, port);
  std::vector<key_t> keys;

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    auto str = ss.str();
    keys.emplace_back(
        reinterpret_cast<const unsigned char *>(str.data()),
        reinterpret_cast<const unsigned char *>(str.data() + str.size()));
  }

  for (size_t i = 0; run.load(); i++) {
    if (i >= keys.size())
      i = 0;
    const auto &key = keys[i];
    if (socket.contains(key)) {
      found++;
    } else {
      notfound++;
    }
  }
}

int main() {
  const size_t max_keys = 512;
  const size_t from_threads = 1;
  const size_t to_threads = 20;
  std::atomic<uint64_t> found(0);
  std::atomic<uint64_t> notfound(0);
  const auto measurement_time = std::chrono::seconds(20);

  for (size_t current_threads = from_threads; current_threads < to_threads;
       current_threads++) {
    std::cout << "Running with " << current_threads << " thread(s) ... ";
    std::cout.flush();

    std::atomic_bool run(true);
    hydra::async([&]() {
      std::this_thread::sleep_for(measurement_time);
      run = false;
    });

    std::vector<std::thread> threads;
    for (size_t i = 0; i < current_threads; i++) {
      threads.emplace_back(get_keys, "10.1", "8042", max_keys, std::ref(run),
                           std::ref(found), std::ref(notfound));
    }

    for (auto &&thread : threads)
      thread.join();

    const uint64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(
        measurement_time).count();
    const uint64_t keys = found.load() + notfound.load();
    std::cout << keys / seconds / 1000 << " kOps/s" << std::endl;
    log_info() << "Found " << found.load() << " keys of " << keys;
    found = 0;
    notfound = 0;
  }
}

