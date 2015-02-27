#include <random>
#include <sstream>
#include <atomic>
#include <thread>
#include <string>
#include <vector>

#include "hydra/passive.h"

static void get_keys(const std::string &host, const std::string &port,
                     const size_t max_keys) {
  using key_t = std::vector<unsigned char>;
  hydra::passive socket(host, port);
  std::vector<key_t> keys;
  keys.reserve(max_keys);

  std::mt19937_64 gen;
  std::uniform_int_distribution<size_t> distribution(0, max_keys - 1);

  std::generate_n(std::back_inserter(keys), max_keys, [&distribution, &gen]() {
    size_t key = distribution(gen);
    std::ostringstream ss;
    ss << std::setw(4) << key;
    auto str = ss.str();
    return key_t(
        reinterpret_cast<const unsigned char *>(str.data()),
        reinterpret_cast<const unsigned char *>(str.data() + str.size()));
  });

  using namespace std::chrono;

  auto start = high_resolution_clock::now();
  for (const auto &key : keys) {
    socket.get(key);
  }
  auto end = high_resolution_clock::now();

  auto ns = duration_cast<milliseconds>(end - start).count();

  std::cout << static_cast<float>(max_keys) / ns << std::endl;
}

int main(int argc, const char *argv[]) {
  const size_t max_keys = (argc < 2) ? 1000 * 300 * 1 : atoi(argv[1]);
  std::cout << max_keys << " keys." << std::endl;
  get_keys("10.10", "8042", max_keys);
}

