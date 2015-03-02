#include <random>
#include <sstream>
#include <iostream>
#include <fstream>
#include <iterator>
#include <iomanip>
#include <algorithm>

int main(int argc, const char *argv[]) {
  constexpr size_t key_length = 4;
  constexpr size_t value_length = 64;
  constexpr size_t kv_pairs = 4 * 1000 * 1000;

  if (argc < 2) {
    std::cerr << "Output filename for kv-pairs missing" << std::endl;
    return EXIT_FAILURE;
  }

  if (argc < 2) {
    std::cerr << "Output filename for keys missing" << std::endl;
    return EXIT_FAILURE;
  }

  std::string kv_file(argv[1]);
  std::string key_file(argv[2]);

  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::ofstream kv(kv_file.c_str(), std::ofstream::out);
  std::ofstream keys(key_file.c_str(), std::ofstream::out);

  for (size_t i = 0; i < kv_pairs; ++i) {
    kv << std::setw(key_length) << i << std::endl;
    keys << std::setw(key_length) << i << std::endl;

    std::generate_n(std::ostream_iterator<unsigned char>(kv), value_length,
                    [&]() { return distribution(generator); });
    kv << std::endl;
  }
}
