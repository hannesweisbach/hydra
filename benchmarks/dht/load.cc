#include <random>
#include <sstream>

#include "hydra/client.h"
#include "hydra/hash.h"

#include "util/Logger.h"

#include "google/profiler.h"

void load_keys(hydra::client &client, const size_t max_keys,
               size_t value_length) {
  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');
  
  struct data {
    std::string key;
    std::unique_ptr<unsigned char[]> value;
    std::future<bool> result;
    data(const std::string &key, std::unique_ptr<unsigned char[]> value)
        : key(key), value(std::move(value)) {}
  };

  std::vector<data> requests;
  requests.reserve(max_keys);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << hydra::hash(&i, sizeof(i));
    std::unique_ptr<unsigned char[]> value =
#if 1
        std::unique_ptr<unsigned char[]>(new unsigned char[value_length]);
#else
        std::make_unique<unsigned char[]>(value_length);
#endif
    for (size_t byte = 0; byte < value_length; byte++) {
      value[byte] = distribution(generator);
    }

    requests.emplace_back(ss.str(), std::move(value));
  }

  auto start = std::chrono::high_resolution_clock::now();

  for (auto &&request : requests) {
    request.result =
        client.add(reinterpret_cast<const unsigned char *>(request.key.c_str()),
                   request.key.size(), request.value.get(), value_length);
    //request.result.wait();
  }

  size_t i = 0;
  for (auto &&request : requests) {
    request.result.get();
    //log_info() << "Finished " << i++;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = end - start;

  Logger logger(Logger::severity_level::info, __func__, __LINE__);
  logger << "Loaded " << max_keys << " key-value pairs in ";

  using us = std::chrono::microseconds;
  using ms = std::chrono::milliseconds;
  using s = std::chrono::seconds;

  if (std::chrono::nanoseconds(duration).count() < 10000) {
    logger << std::chrono::nanoseconds(duration).count() << " ns";
  } else if (std::chrono::duration_cast<us>(duration).count() < 10000) {
    logger << std::chrono::duration_cast<us>(duration).count() << " µs";
  } else if (std::chrono::duration_cast<ms>(duration).count() < 10000) {
    logger << std::chrono::duration_cast<ms>(duration).count() << " ms";
  } else {
    logger << std::chrono::duration_cast<s>(duration).count() << " s";
  }

}

int main() {
  const size_t max_keys = 1024;

  hydra::client client("10.1", "8042");

  ProfilerStart("./load.prof");
  load_keys(client, max_keys, 64);
  ProfilerStop();
}
