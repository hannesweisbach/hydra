#include <random>
#include <sstream>
#include <atomic>
#include <thread>
#include <iostream>

#include "rdma/RDMAClientSocket.h"
#include "protocol/message.h"

#include "hydra/allocators/ZoneHeap.h"
#include "hydra/RDMAAllocator.h"

using response_t = kj::FixedArray<capnp::word, 9>;
struct data {
  rdma_ptr<response_t> response;
  rdma_ptr<unsigned char> kv;
  size_t i = 0;
  size_t length;
  size_t key_length;
  data(rdma_ptr<response_t> response, rdma_ptr<unsigned char> kv,
       const size_t length, const size_t key_length)
      : response(std::move(response)), kv(std::move(kv)), length(length),
        key_length(key_length) {}
};

using iterator = std::vector<data>::iterator;

#if 0
static hydra::future<future<void> > send(RDMAClientSocket &socket,
                                         iterator current, const iterator end) {
  promise<void> promise;
  auto result = promise.get_future();
  socket.recv_async(current->response.first, current->response.second)
      .then([current, end, promise = std::move(promise)](auto &&result) {
         result.value();
         auto &&request = *current;
         std::cout << "Received " << static_cast<void *>(&request) << " "
                   << request.i << std::endl;
         auto reply = capnp::FlatArrayMessageReader(*request.response.first);
         auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

         assert(reader.getAck().getSuccess());

         if (current != end) {
            send(socket, ++current, end);
         }
       });

  socket.send(put_message(request.kv, request.length, request.key_length));

  return result;
}
#endif
static hydra::future<void> send(RDMAClientSocket &socket, data &request) {
  auto result =
      socket.recv_async(*request.response.first, request.response.second)
          .then([&request](auto &&result) {
             result.value();
             auto reply =
                 capnp::FlatArrayMessageReader(*request.response.first);
             auto reader = reply.getRoot<hydra::protocol::DHTResponse>();

             assert(reader.getAck().getSuccess());
           });

  socket.send(put_message(request.kv, request.length, request.key_length));
  return result;
}

static void load_keys(const std::string &host, const std::string &port,
                      const size_t max_keys, const size_t value_length) {
  RDMAClientSocket socket(host, port);
  socket.connect();
  hydra::ZoneHeap<RdmaHeap<ibv_access::READ>, 1024 * 1024 * 16> heap(socket);

  std::mt19937_64 generator;
  std::uniform_int_distribution<unsigned char> distribution(' ', '~');

  std::vector<data> requests;
  requests.reserve(max_keys);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    const size_t key_length = ss.str().size();

    const size_t length = key_length + value_length;
    requests.emplace_back(heap.malloc<response_t>(1),
                          heap.malloc<unsigned char>(length), length,
                          key_length);

    data &data = requests.back();
    memcpy(data.kv.first.get(), ss.str().c_str(), key_length);
    unsigned char *value = data.kv.first.get() + key_length;
    for (size_t byte = 0; byte < value_length; byte++) {
      value[byte] = distribution(generator);
    }
    data.i = i;
  }

  std::cout << requests.size() << std::endl;

  hydra::promise<void> p;
  hydra::future<void> future = p.get_future();

  for (auto &&request : requests) {
    hydra::future<hydra::future<void> > f =
        future.then([&socket, &request](auto &&result) {
          result.value();
          return send(socket, request);
        });

    f.unwrap();

    /* proxy */
    hydra::promise<void> promise;
    future = promise.get_future();
    /* unwrap */
    f.then([promise = std::move(promise)](auto && inner_future) mutable {
      try {
        inner_future.value()
            .then([promise = std::move(promise)](auto && value) mutable {
               try {
                 value.value();
                 promise.set_value();
               }
               catch (...) {
                 promise.set_exception(std::current_exception());
               }
             });
      }
      catch (...) {
        promise.set_exception(std::current_exception());
      }
    });
  }

  auto start = std::chrono::high_resolution_clock::now();

  p.set_value();
  future.get();

  auto end = std::chrono::high_resolution_clock::now();

  auto duration = end - start;

  size_t ops = 0;
  auto time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  std::cout << time << "ns" << std::endl;

  std::string unit;
  if (auto seconds =
          std::chrono::duration_cast<std::chrono::seconds>(duration).count()) {
    ops = max_keys / seconds;
    unit = "s";
  } else if (auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 duration).count()) {
    ops = max_keys / ms;
    unit = "ms";
  } else if (auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                 duration).count()) {
    ops = max_keys / us;
    unit = "us";
  }

  std::cout << ops << " Ops/" << unit << std::endl;
}

int main(int argc, char const *const argv[]) {
  const size_t value_length = (argc < 2) ? 64 : atoi(argv[1]);
  const size_t max_keys = 1000 * 10;
  const size_t min_threads = 1;
  const size_t max_threads = 1;

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

      std::generate_n(std::back_inserter(threads), cur_threads, [=]() {
        return std::thread(load_keys, "10.10", "8042", max_keys, value_length);
      });

      for (auto &&thread : threads) {
        thread.join();
      }
    }
  }
}

