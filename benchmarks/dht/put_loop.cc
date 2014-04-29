#include <random>
#include <sstream>
#include <atomic>
#include <thread>

#include "hydra/client.h"
#include "hydra/hash.h"

#include "util/Logger.h"

#include "google/profiler.h"

struct data {
  rdma_ptr<unsigned char> key;
  rdma_ptr<unsigned char> value;
  size_t key_length;
  size_t value_length;
  std::pair<std::future<qp_t>, rdma_ptr<put_response> > result;
  data(rdma_ptr<unsigned char> key, rdma_ptr<unsigned char> value,
       const size_t key_length, const size_t value_length)
      : key(std::move(key)), value(std::move(value)), key_length(key_length),
        value_length(value_length) {}
};

static void send(RDMAClientSocket &socket, data &request) {
  put_request r = { { request.key.first.get(), request.key_length,
                      request.key.second->rkey },
                    { request.value.first.get(), request.value_length,
                      request.value.second->rkey } };

  request.result = socket.recv_async<put_response>();
  socket.sendImmediate(r);
}

static void load_keys(RDMAClientSocket &socket, const size_t max_keys,
                      const size_t value_length, std::atomic_bool &run,
                      std::atomic<uint64_t> &cnt) {
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

  for (auto &&request : requests) {
    send(socket, request);
    cnt++;
  }

  for (size_t i = 0; run.load(); i++) {
    if (i >= requests.size())
      i = 0;
    auto &&request = requests[i];
    request.result.first.get();
    assert(request.result.second.first->value());
    send(socket, request);
    cnt++;
  }
}

int main() {
  std::atomic<uint64_t> cnt(0);
  const size_t max_keys = 512;
  const auto measurement_time = std::chrono::seconds(20);

  RDMAClientSocket socket("10.1", "8042");
  socket.connect();

  //  ProfilerStart("./load.prof");
  // load_keys(client, max_keys, 64);
  for (const auto &value_size : { 64, 128, 256, 1024, 4096 }) {
    std::atomic_bool run(true);
    hydra::async([&]() {
      std::this_thread::sleep_for(measurement_time);
      run = false;
    });
    log_info() << "Running with a value_size of " << value_size;
    load_keys(socket, max_keys, value_size, run, cnt);
    const uint64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(
        measurement_time).count();
    log_info() << "kOps/s: " << cnt.fetch_and(0) / seconds / 1000;
  }

  ProfilerStop();
}

