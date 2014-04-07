#include <string>
#include <chrono>
#include <random>

#include <getopt.h>
#include <unistd.h>
#include <assert.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include "Logger.h"
#include "utils.h"
#include "hydra/client.h"

std::unique_ptr<unsigned char[]> get_random_string(size_t length) {
  static std::mt19937_64 generator;
  static std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::unique_ptr<unsigned char[]> random(new unsigned char[length]);
  for(size_t i = 0; i < length; i++)
    random[i] = distribution(generator);

  return random;
}

bool test_add(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  log_hexdump_ptr(key.get(), key_size);

  return c.add(key.get(), key_size, value.get(), val_size).get();
}

bool test_add_contains(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  log_hexdump_ptr(key.get(), key_size);
  
  assert(c.add(key.get(), key_size, value.get(), val_size).get());
  return c.contains(key.get(), key_size);
}

bool test_add_get(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  assert(c.add(key.get(), key_size, value.get(), val_size).get());
  hydra::client::value_ptr p = c.get(key.get(), key_size);
  assert(p.get() != nullptr);
  return memcmp(p.get(), value.get(), val_size) == 0;
}

bool test_double_add(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 16;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  
  /* add key/value pair, check contains and get */
  c.add(key.get(), key_size, value.get(), val_size).get();
  assert(c.contains(key.get(), key_size));
  hydra::client::value_ptr p = c.get(key.get(), key_size);
  assert(p.get() != nullptr);
  assert(memcmp(p.get(), value.get(), val_size) == 0);
   
  //std::this_thread::sleep_for(std::chrono::seconds(1));
  
  /* add another value w/ same key, check contains and get */
  value = get_random_string(val_size);
  c.add(key.get(), key_size, value.get(), val_size).get();
  assert(c.contains(key.get(), key_size));
  p = c.get(key.get(), key_size);
  assert(p.get() != nullptr);
  return memcmp(p.get(), value.get(), val_size) == 0;
}

bool test_add_remove(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  assert(c.add(key.get(), key_size, value.get(), val_size).get());
  assert(c.contains(key.get(), key_size));
  hydra::client::value_ptr p = c.get(key.get(), key_size);
  assert(p.get() != nullptr);
  assert(memcmp(p.get(), value.get(), val_size) == 0);

  assert(c.remove(key.get(), key_size).get());

  return !c.contains(key.get(), key_size);
}


bool test_grow(hydra::passive& c) {
  const size_t key_size = 16;
  const size_t val_size = 16;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  size_t old_size = c.size();
  for(size_t i = 0; i < old_size + 1; i++) {
    std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
    c.add(key.get(), key_size, value.get(), val_size).get();
    assert(c.contains(key.get(), key_size));
    //std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return c.size() > old_size;
}

int main(int argc, char * const argv[]) {
  static struct option long_options[] = {
    { "port",      required_argument, 0, 'p' },
    { "interface", required_argument, 0, 'i' },
    { "verbosity", optional_argument, 0, 'v' },
    { 0, 0, 0, 0 }
  };

  std::string host;
  std::string port("8042");
  int verbosity = -1;

  while (1) {
    int option_index = 0;
    int c = getopt_long(argc, argv, "p:i:", long_options, &option_index);

    if(c == -1)
      break;
    switch(c) {
      case 'p':
        port = optarg; break;
      case 'i':
        host = optarg; break;
      case 'v':
        if(optarg) {
          /* TODO: parse optarg for level */
        } else {
          verbosity++;
        }
        break;
      case '?':
      default:
       log_info() << "Unkown option code " << (char)c;
    }
  }

  Logger::set_severity(verbosity);
  
  log_info() << "Starting on interface " << host << ":" << port;
  hydra::client c(host, port);
  
  //std::this_thread::sleep_for(std::chrono::minutes(60));

  log_info() << "Starting test test_add";
  assert(test_add(c));
  log_info() << "Test test_add done";

  log_info() << "Starting test test_add_contains";
  assert(test_add_contains(c));
  log_info() << "Test test_add_contains done";
 
  log_info() << "Starting test test_add_get";
  assert(test_add_get(c));
  log_info() << "Test test_add_get done";
  
  log_info() << "Starting test test_double_add";
  assert(test_double_add(c));
  log_info() << "Test test_double_add done";

  log_info() << "Starting test test_add_remove";
  assert(test_add_remove(c));
  log_info() << "Test test_add_remove done";

  log_info() << "Starting test test_grow";
  assert(test_grow(c));
  log_info() << "Test test_grow done";

}

