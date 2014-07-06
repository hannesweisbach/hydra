#include <random>
#include <sstream>
#include <atomic>
#include <thread>

#include "hydra/client.h"
#include "protocol/message.h"
#include "rdma/RDMAClientSocket.h"

typedef std::unique_ptr<unsigned char, std::function<void(unsigned char *)> >
value_ptr;

std::pair<value_ptr, const size_t> find_entry(const RDMAClientSocket &socket,
                                              const unsigned char *key,
                                              const size_t key_length,
                                              const hydra::node_info &info) {
  const size_t table_size = info.table_size;
  const RDMAObj<hydra::hash_table_entry> *remote_table =
      static_cast<const RDMAObj<hydra::hash_table_entry> *>(
          info.key_extents.addr);
  const uint32_t rkey = info.key_extents.rkey;

  size_t index = hydra::hash(key, key_length) % table_size;

  auto mem = socket.from(&remote_table[index], info.key_extents.rkey);
  auto &entry = mem.first->get();

  for (size_t hop = entry.hop; hop; hop >>= 1) {
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
    socket.reload(mem, &remote_table[++index % table_size], rkey);
  }

  return { value_ptr(nullptr, [](void *) {}), 0 };
}

static void get_keys(RDMAClientSocket &socket, const size_t max_keys,
                     std::atomic_bool &run, std::atomic<uint64_t> &found,
                     std::atomic<uint64_t> &notfound) {
  const hydra::node_info info = hydra::get_info(socket);
  std::vector<std::string> keys;

  for (size_t i = 0; i < max_keys; i++) {
    std::ostringstream ss;
    ss << std::setw(4) << i;

    keys.push_back(ss.str());
  }

  for (size_t i = 0; run.load(); i++) {
    if (i >= keys.size())
      i = 0;
    const auto &key = keys[i];
    auto result =
        find_entry(socket, reinterpret_cast<const unsigned char *>(key.c_str()),
                   key.size(), info);
    if (result.first) {
      found++;
    } else {
      notfound++;
    }
  }
}

int main() {
  const size_t max_keys = 512;
  const size_t from_threads = 1;
  const size_t to_threads = 20;
  std::atomic<uint64_t> found(0);
  std::atomic<uint64_t> notfound(0);
  const auto measurement_time = std::chrono::seconds(20);

  for (size_t current_threads = from_threads; current_threads < to_threads;
       current_threads++) {
    RDMAClientSocket socket("10.1", "8042");
    socket.connect();

    std::cout << "Running with " << current_threads << " thread(s) ... ";
    std::cout.flush();

    std::atomic_bool run(true);
    hydra::async([&]() {
      std::this_thread::sleep_for(measurement_time);
      run = false;
    });

    std::vector<std::thread> threads;
    for (size_t i = 0; i < current_threads; i++) {
      threads.emplace_back(get_keys, std::ref(socket), max_keys, std::ref(run),
                           std::ref(found), std::ref(notfound));
    }

    for (auto &&thread : threads)
      thread.join();

    const uint64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(
        measurement_time).count();
    const uint64_t keys = found.load() + notfound.load();
    std::cout << keys / seconds / 1000 << " kOps/s" << std::endl;
    log_info() << "Found " << found.load() << " keys of " << keys;
    found = 0;
    notfound = 0;
  }
}

