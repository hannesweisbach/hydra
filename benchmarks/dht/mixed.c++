#include <random>
#include <sstream>
#include <atomic>
#include <thread>
#include <string>
#include <vector>

#include "hydra/passive.h"

namespace hydra {
using request_t = kj::Array<capnp::word>;
using key_t = std::vector<unsigned char>;
}

hydra::key_t generate_key(std::mt19937_64 &gen,
                          std::uniform_int_distribution<size_t> &distribution) {
  size_t key = distribution(gen);
  std::ostringstream ss;
  ss << std::setw(4) << key;
  auto str = ss.str();
  return hydra::key_t(
      reinterpret_cast<const unsigned char *>(str.data()),
      reinterpret_cast<const unsigned char *>(str.data() + str.size()));
}

void append_value(hydra::key_t &key, std::mt19937_64 &generator) {
  static std::uniform_int_distribution<unsigned char> distribution(' ', '~');
  std::generate_n(std::back_inserter(key), 64,
                  [&]() { return distribution(generator); });
}

struct request {
  hydra::key_t kv;
  size_t key_size;
};

static void get_keys(const std::string &host, const std::string &port,
                     const size_t max_keys, const unsigned get_percentage) {
  using namespace std::chrono;
  hydra::passive socket(host, port);
  std::vector<request> requests;
  requests.reserve(max_keys);

  std::mt19937_64 gen;
  std::uniform_int_distribution<size_t> distribution(0, max_keys - 1);
  std::uniform_int_distribution<unsigned> request_type(0, 100);

  std::generate_n(
      std::back_inserter(requests), max_keys,
      [&gen, &distribution, &request_type, get_percentage ]()->request {
        if (request_type(gen) < get_percentage) {
          return { generate_key(gen, distribution), 0 };
        } else {
          auto kv = generate_key(gen, distribution);
          auto key_size = kv.size();

          append_value(kv, gen);
          return { kv, key_size };
        }
      });

  auto start = high_resolution_clock::now();
  for (const auto &request : requests) {
    if (request.key_size) {
      socket.put(request.kv, request.key_size);
    } else {
      socket.get(request.kv);
    }
  }
  auto end = high_resolution_clock::now();

  auto ns = duration_cast<milliseconds>(end - start).count();

  std::cout << static_cast<float>(max_keys) / ns << std::endl;
}

int main(int argc, const char *argv[]) {
  const size_t max_keys = (argc < 2) ? 1000 * 300 * 1 : atoi(argv[1]);
  const unsigned percentage = (argc < 3) ? 90 : atoi(argv[2]);
  std::cout << max_keys << " keys." << std::endl;
  std::cout << percentage << "% GET, " << 100 - percentage << "% PUT"
            << std::endl;
  get_keys("10.10", "8042", max_keys, percentage);
}


