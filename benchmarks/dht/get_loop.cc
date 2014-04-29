#include <random>
#include <sstream>
#include <atomic>
#include <thread>

#include "hydra/client.h"
#include "hydra/hash.h"

#include "util/Logger.h"

#include "google/profiler.h"

typedef std::unique_ptr<unsigned char, std::function<void(unsigned char *)> >
value_ptr;

hydra::node_info get_info(const RDMAClientSocket &socket) {
  auto init = socket.recv_async<mr_response>();
  socket.sendImmediate(init_request());

  init.first.get(); // block.
  auto mr = init.second.first->value();
  assert(mr.size >= sizeof(hydra::node_info));
  auto info = socket.read<hydra::node_info>(mr.addr, mr.rkey);
  info.first.get(); // block

  return *info.second.first;
}

std::pair<value_ptr, const size_t> find_entry(const RDMAClientSocket &socket,
                                              const unsigned char *key,
                                              const size_t key_length,
                                              const hydra::node_info &info) {
  const size_t table_size = info.table_size;
  const RDMAObj<hydra::key_entry> *remote_table =
      static_cast<const RDMAObj<hydra::key_entry> *>(info.key_extents.addr);
  const uint32_t rkey = info.key_extents.rkey;

  const size_t index = hydra::hash(key, key_length) % table_size;

  auto mem = socket.from(&remote_table[index], info.key_extents.rkey);
  auto &entry = mem.first->get();

  for (size_t hop = entry.hop, d = 1; hop; hop >>= 1, d++) {
    if ((hop & 1) && !entry.is_empty() && (key_length == entry.key_length())) {
      auto data = socket.malloc<unsigned char>(entry.ptr.size);
      uint64_t crc = 0;
      do {
        socket.read(data.first.get(), data.second, entry.key(), entry.rkey,
                    entry.ptr.size).get();
        crc = hydra::hash64(data.first.get(), entry.ptr.size);
      } while (entry.ptr.crc != crc);
      if (memcmp(data.first.get(), key, key_length) == 0) {
        return { std::move(data.first), entry.value_length() };
      }
    }
    const size_t next_index = (index + d) % table_size;
    socket.reload(mem, &remote_table[next_index], rkey);
  }

  return { value_ptr(nullptr, [](void *) {}), 0 };
}

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

static void get_keys(RDMAClientSocket &socket, const size_t max_keys,
                     std::atomic_bool &run, std::atomic<uint64_t> &cnt) {
  const hydra::node_info info = get_info(socket);

  std::vector<std::string> keys;
  std::vector<data> requests;
  requests.reserve(max_keys);

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    keys.push_back(ss.str());
  }

  for (size_t i = 0; run.load(); i++) {
    if (i >= keys.size())
      i = 0;
    const auto &key = keys[i];
    find_entry(socket, reinterpret_cast<const unsigned char *>(key.c_str()),
               key.size(), info);
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
  std::atomic_bool run(true);
  hydra::async([&]() {
    std::this_thread::sleep_for(measurement_time);
    run = false;
  });
  get_keys(socket, max_keys, run, cnt);
  const uint64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(
      measurement_time).count();
  log_info() << "kOps/s: " << cnt.fetch_and(0) / seconds / 1000;

  ProfilerStop();
}

