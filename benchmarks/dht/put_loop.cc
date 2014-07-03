#include <random>
#include <sstream>
#include <atomic>
#include <thread>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"

#include "hydra/client.h"
#include "hydra/hash.h"

#include "util/Logger.h"

namespace hydra {
namespace rdma {
template <> const void *address_of(const kj::Array<capnp::word> &o) {
  return o.begin();
}

template <> size_t size_of(const kj::Array<capnp::word> &o) {
  return o.size() * sizeof(capnp::word);
}
}
}

struct data {
  rdma_ptr<unsigned char> key;
  rdma_ptr<unsigned char> value;
  size_t key_length;
  size_t value_length;
  data(rdma_ptr<unsigned char> key, rdma_ptr<unsigned char> value,
       const size_t key_length, const size_t value_length)
      : key(std::move(key)), value(std::move(value)), key_length(key_length),
        value_length(value_length) {}
};

static void send(RDMAClientSocket &socket, data &request) {
  ::capnp::MallocMessageBuilder message;
  hydra::protocol::DHTRequest::Builder msg =
      message.initRoot<hydra::protocol::DHTRequest>();

  auto put = msg.initPut();
  auto remote = put.initRemote();
  auto key = remote.initKey();
  key.setAddr(reinterpret_cast<uint64_t>(request.key.first.get()));
  key.setSize(request.key_length);
  key.setRkey(request.key.second->rkey);
  auto value = remote.initValue();
  value.setAddr(reinterpret_cast<uint64_t>(request.value.first.get()));
  value.setSize(request.value_length);
  value.setRkey(request.value.second->rkey);

  auto result = socket.recv_async<kj::FixedArray<capnp::word, 9> >();

  kj::Array<capnp::word> serialized = messageToFlatArray(message);
  socket.sendImmediate(serialized);
  result.first.get();

  auto reply = capnp::FlatArrayMessageReader(*result.second.first);
  auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

  assert(reader.getAck().getSuccess());
}

static void load_keys(const std::string &host, const std::string &port,
                      const size_t max_keys, const size_t value_length,
                      std::atomic_bool &run, std::atomic<uint64_t> &cnt) {
  RDMAClientSocket socket(host, port);
  socket.connect();
  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::vector<data> requests;
  requests.reserve(max_keys);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    const size_t key_length = ss.str().size();

    requests.emplace_back(socket.malloc<unsigned char>(key_length),
                          socket.malloc<unsigned char>(value_length),
                          key_length, value_length);

    data &data = requests.back();
    memcpy(data.key.first.get(), ss.str().c_str(), key_length);
    unsigned char *value = data.value.first.get();
    for (size_t byte = 0; byte < value_length; byte++) {
      value[byte] = distribution(generator);
    }
  }

  for (;;) {
    for (auto &&request : requests) {
      send(socket, request);
      cnt++;
      if (!run)
        return;
    }
  }
}

int main(int argc, char const *const argv[]) {
  const size_t value_length = (argc < 2) ? 64 : atoi(argv[1]);
  const size_t max_keys = 255;
  const size_t min_threads = 1;
  const size_t max_threads = 20;
  const auto measurement_time = std::chrono::seconds(2);

  static_assert(min_threads <= max_threads,
                "The minimum number of threads must "
                "be smaller than the maximum number");

  std::atomic<uint64_t> cnt(0);

  for (const auto &value_size : { value_length /*64, 128, 256, 1024, 4096*/ }) {
    for (size_t cur_threads = min_threads; cur_threads <= max_threads;
         cur_threads++) {
      std::atomic_bool run(true);

      hydra::async([&]() {
        std::this_thread::sleep_for(measurement_time);
        run = false;
      });

      std::cout << "Running with a value_size of " << value_size << " and "
                << cur_threads << " threads ... ";
      std::cout.flush();

      std::vector<std::thread> threads;
      threads.reserve(cur_threads);

      std::generate_n(std::back_inserter(threads), cur_threads, [&]() {
        return std::thread(load_keys, "10.1", "8042", max_keys, value_length,
                           std::ref(run), std::ref(cnt));
      });

      for (auto &&thread : threads) {
        thread.join();
      }

      const uint64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(
          measurement_time).count();
      std::cout << "kOps/s: " << cnt.fetch_and(0) / seconds / 1000 << std::endl;
    }
  }
}

