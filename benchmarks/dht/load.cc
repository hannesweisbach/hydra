#include <random>
#include <sstream>
#include <vector>

#include "rdma/RDMAClientSocket.h"
#include "protocol/message.h"

static void load_keys(RDMAClientSocket &socket, const size_t max_keys,
                      size_t value_length) {
  using buffer_t = kj::FixedArray<capnp::word, 9>;
  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  struct data {
    rdma_ptr<unsigned char> kv;
    size_t key_length;
    size_t length;
    const buffer_t &buffer;
    std::future<qp_t> result;
    data(rdma_ptr<unsigned char> kv, const size_t length,
         const size_t key_length, const buffer_t &buffer)
        : kv(std::move(kv)), key_length(key_length), length(length),
          buffer(buffer) {}
  };

  std::vector<data> requests;
  requests.reserve(max_keys);

  std::vector<buffer_t> buffers(max_keys);
  auto buffer_mr = socket.register_memory(ibv_access::MSG, buffers);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    const size_t key_length = ss.str().size();
    const size_t length = key_length + value_length;
    requests.emplace_back(socket.malloc<unsigned char>(length), length,
                          key_length, buffers.at(i));

    data &data = requests.back();
    memcpy(data.kv.first.get(), ss.str().c_str(), key_length);
    unsigned char *value = data.kv.first.get() + key_length;
    for (size_t byte = 0; byte < value_length; byte++) {
      value[byte] = distribution(generator);
    }
  }

  auto start = std::chrono::high_resolution_clock::now();

  for (auto &&request : requests) {
    request.result = socket.recv_async(request.buffer, buffer_mr.get());

    auto serialized =
        put_message(request.kv, request.length, request.key_length);
    socket.send(serialized);
  }

  for (auto &&request : requests) {
    request.result.get();
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
  const size_t max_keys = 128;

  RDMAClientSocket socket("10.1", "8042");
  socket.connect();

  load_keys(socket, max_keys, 64);
}
