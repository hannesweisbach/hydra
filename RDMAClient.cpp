#include <string>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>

#include <getopt.h>
#include <unistd.h>
#include <assert.h>

#include "Logger.h"
#include "utils.h"
#include "hydra/client.h"
#include "rdma/RDMAClientSocket.h"
#include "hydra/protocol/message.h"

std::vector<unsigned char> get_random_string(size_t length) {
  static std::mt19937_64 generator;
  static std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::vector<unsigned char> kv;
  kv.reserve(length);
  std::generate_n(std::back_inserter(kv), length,
                  std::bind(distribution, generator));

  return kv;
}

bool test_add(hydra::client &c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  auto key = get_random_string(key_size);
  auto value = get_random_string(val_size);

  log_hexdump_ptr(key.data(), key.size());

  return c.add(key, value);
}

bool test_add_contains(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  auto key = get_random_string(key_size);
  auto value = get_random_string(val_size);

  log_hexdump_ptr(key.data(), key.size());

  assert(c.add(key, value));
  return c.contains(key);
}

bool test_add_get(hydra::client &c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  auto key = get_random_string(key_size);
  auto value = get_random_string(val_size);

  assert(c.add(key, value));

  auto val = c.get(key);
  assert(!val.empty());
  return std::equal(std::begin(value), std::end(value), val.data());
}

bool test_double_add(hydra::client &c) {
  const size_t key_size = 16;
  const size_t val_size = 16;
  auto key = get_random_string(key_size);
  auto value = get_random_string(val_size);

  /* add key/value pair, check contains and get */
  assert(c.add(key, value));
  assert(c.contains(key));
  auto val = c.get(key);
  assert(!val.empty());
  assert(std::equal(std::begin(value), std::end(value), val.data()));

  // std::this_thread::sleep_for(std::chrono::seconds(1));

  /* add another value w/ same key, check contains and get */
  value = get_random_string(val_size);
  assert(c.add(key, value));
  assert(c.contains(key));
  val = c.get(key);
  assert(!val.empty());
  return std::equal(std::begin(value), std::end(value), val.data());
}

bool test_add_remove(hydra::client &c) {
  const size_t key_size = 16;
  const size_t val_size = 243;
  auto key = get_random_string(key_size);
  auto value = get_random_string(val_size);

  assert(c.add(key, value));
  assert(c.contains(key));
  auto val = c.get(key);
  assert(!val.empty());
  assert(std::equal(std::begin(value), std::end(value), val.data()));

  assert(c.remove(key));

  return !c.contains(key);
}

auto find_key_in(const size_t key_size, const hydra::keyspace_t &start,
                 const hydra::keyspace_t &end) {
  for (;;) {
    auto key = get_random_string(key_size);
    if (hydra::keyspace_t(hydra::hash(key.data(), key.size())).in(start, end))
      return key;
  }
}

bool test_wrong_add(const std::string &host, const std::string &port) {
  hydra::chord::node node(host, port);

  auto self = node.self();
  auto pred = node.predecessor(self.id).id;
  if (self.id == pred) {
    log_info() << "Could not perform test. Node is responsible for everything.";
    return true;
  }

  hydra::keyspace_t start = self.id + hydra::keyspace_t(1);
  hydra::keyspace_t end = pred;

  const size_t key_size = 16;
  const size_t val_size = 16;
  auto value = get_random_string(val_size);
  auto key = find_key_in(key_size, start, end);

  key.insert(std::end(key), std::begin(value), std::end(value));

  hydra::passive passive(self.ip, self.port);
  return passive.put(key, key_size);
}

#if 0
bool test_grow(hydra::client& c) {
  const size_t key_size = 16;
  const size_t val_size = 16;
  std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
  std::unique_ptr<unsigned char[]> value(get_random_string(val_size));

  size_t old_size = c.size();
  for(size_t i = 0; i < old_size + 1; i++) {
    std::unique_ptr<unsigned char[]> key(get_random_string(key_size));
    c.add(key.get(), key_size, value.get(), val_size);
    assert(c.contains(key.get(), key_size));
    //std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return c.size() > old_size;
}
#endif

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

  log_info() << "Starting test_wrong_add";
  assert(test_wrong_add(host, port));
  log_info() << "Test test_wrong_add done";

#if 0
  log_info() << "Starting test test_grow";
  assert(test_grow(c));
  log_info() << "Test test_grow done";
#endif

}

