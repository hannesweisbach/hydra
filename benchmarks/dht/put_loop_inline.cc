#include <random>
#include <sstream>
#include <atomic>
#include <thread>
#include <algorithm>
#include <memory>
#include <chrono>
#include <algorithm>
#include <vector>

#include "protocol/message.h"
#include "rdma/RDMAClientSocket.h"

#ifdef PROFILER
#include <gperftools/profiler.h>
#endif

#include "util/stat.h"

using response = kj::FixedArray<capnp::word, 9>;

static thread_local mr_t response_mr;

struct data {
  kj::Array<capnp::word> request;
  response *response;
};

static ecdf measurement;

static void send(RDMAClientSocket &socket, data &request) {
  auto result = socket.recv_async(*request.response, response_mr.get());
  socket.send(request.request);

  // measurement.start();
  result.get();
  // measurement.end();

  auto reply = capnp::FlatArrayMessageReader(*request.response);

  assert(reply.getRoot<hydra::protocol::DHTResponse>().getAck().getSuccess());
}

static void load_keys(const std::string &host, const std::string &port,
                      const size_t max_keys, const size_t value_length) {
  RDMAClientSocket socket(host, port);
  socket.connect();
  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::vector<response> responses(max_keys);
  response_mr = socket.register_memory(ibv_access::READ, responses.data(),
                                       responses.size() * sizeof(response));

  std::vector<data> requests;
  requests.reserve(max_keys);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    const size_t key_length = ss.str().size();
    std::vector<unsigned char> kv(key_length);
    memcpy(kv.data(), ss.str().c_str(), key_length);

    assert(key_length + value_length < 256);

    std::generate_n(std::back_inserter(kv), value_length,
                    [&]() { return distribution(generator); });

    requests.emplace_back(
        data{ put_message_inline(kv, static_cast<uint8_t>(key_length)),
              &responses.at(i) });
  }

  auto start = std::chrono::high_resolution_clock::now();

#ifdef PROFILER
  ProfilerStart("./put_inline.profile");
#endif

  for (auto &&request : requests) {
    send(socket, request);
  }

#ifdef PROFILER
  ProfilerStop();
#endif

  auto end = std::chrono::high_resolution_clock::now();

  auto duration = end - start;

  size_t ops = 0;
  auto time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  std::cout << time << "ns" << std::endl;

  auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(duration).count();
  auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  auto us =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if (seconds > 100) {
    ops = max_keys / seconds;
  } else if (ms > 100) {
    ops = max_keys * 1000 / ms;
    std::cout << "ms " << ms << std::endl;
  } else if (us > 100) {
    ops = max_keys * 1000 * 1000 / us;
  }

  std::cout << ops << " Ops/s" << std::endl;
}

int main(int argc, char const *const argv[]) {
  const size_t max_keys = (argc < 2) ? 1000 * 300 * 1 : atoi(argv[1]);
  const size_t value_length = (argc < 3) ? 64 : atoi(argv[2]);
  const size_t min_threads = 1;
  const size_t max_threads = 1;

  std::cout << max_keys << " keys with " << value_length << " bytes."
            << std::endl;
  static_assert(min_threads <= max_threads,
                "The minimum number of threads must "
                "be smaller than the maximum number");

  for (const auto &value_size : { value_length /*64, 128, 256, 1024, 4096*/ }) {
    for (size_t cur_threads = min_threads; cur_threads <= max_threads;
         cur_threads++) {
      std::cout << "Running with a value_size of " << value_size << " and "
                << cur_threads << " threads ... ";
      std::cout.flush();

      std::vector<std::thread> threads;
      threads.reserve(cur_threads);

      std::generate_n(std::back_inserter(threads), cur_threads, [&]() {
        return std::thread(load_keys, "10.10", "8042", max_keys, value_length);
      });

      for (auto &&thread : threads) {
        thread.join();
      }
    }
  }

  std::cout << measurement << std::endl;
}

