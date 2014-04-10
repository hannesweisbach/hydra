#include <chrono>
#include <typeinfo>

#include <unordered_map>
#include <algorithm>

#include "hash.h"
#include "passive.h"
#include "messages.h"
#include "util/Logger.h"
#include "util/concurrent.h"


auto size2Class = [](size_t size) -> size_t {
  if (size == 0)
    return 0;
  if (size <= 128) // 16 bins
    return ((size + (8 - 1)) & ~(8 - 1)) / 8 - 1;
  else if (size <= 4096) // 31 bins
    return ((size + (128 - 1)) & ~(128 - 1)) / 128 + 14;
  else
    return 47 + hydra::util::log2(size - 1) -
           hydra::util::static_log2<4096>::value;
};

hydra::passive::passive(const std::string &host, const std::string &port)
    : s(host, port), heap(48U, size2Class, s), local_heap(s),
      msg_buffer(local_heap.malloc<msg>(2)),
      info(local_heap.malloc<node_info>()) {
  log_info() << "Starting client to " << host << ":" << port;
  post_recv(msg_buffer.first.get()[0], msg_buffer.second);
  post_recv(msg_buffer.first.get()[1], msg_buffer.second);

  s.connect();


  init_request request;
  auto future = request.set_completion<const mr &>([&](auto &&mr) {
    remote = mr;
    this->update_info();
  });

  s.sendImmediate(request);
  future.wait();
}

hydra::passive::passive(hydra::passive &&other)
    : s(std::move(other.s)), heap(std::move(other.heap)),
      local_heap(std::move(other.local_heap)),
      msg_buffer(std::move(other.msg_buffer)), info(std::move(other.info)) {}

hydra::passive &hydra::passive::operator=(hydra::passive &&other) {
  std::swap(s, other.s);
  std::swap(heap, other.heap);
  std::swap(local_heap, other.local_heap);
  std::swap(msg_buffer, other.msg_buffer);
  std::swap(info, other.info);

  return *this;
}

std::future<void> hydra::passive::post_recv(const msg &m,
                                           const ibv_mr *mr) {
  auto fut = s.recv_async(m, mr);
#if 0
  return hydra::then(std::move(fut), [=,&m](auto m_) mutable {
    m_.get(); // check for exception
#else
  return messageThread.send([this, &m, mr, future=std::move(fut)]()mutable{
#endif
  future.get();
  recv(m);
  post_recv(m, mr);
});
}

#if 0
void print_distribution(std::unordered_map<uint64_t, uint64_t> &distribution) {
  std::vector<std::pair<uint64_t, uint64_t> > v(std::begin(distribution),
                                                std::end(distribution));
  std::sort(std::begin(v), std::end(v),
            [](auto rhs, auto lhs) { return rhs.first < lhs.first; });
  for (auto &&e : v) {
    log_info() << e.first << ": " << e.second;
  }
}
#endif

void hydra::passive::update_info() {
  log_info() << "remote mr: " << remote;
  s.read(info.first.get(), info.second,
         reinterpret_cast<node_info *>(remote.addr), remote.rkey).get();
}

void hydra::passive::recv(const msg &r) {
  log_info() << r;

  switch (r.type()) {
  case msg::mtype::response:
    // TODO: This is only required to avoid deadlock when destructing this
    // object from the completion.
    hydra::async([=]() { static_cast<const response &>(r).complete_(); });
    break;
  case msg::mtype::notification:
    switch (r.subtype()) {
    case msg::msubtype::resize: {
      update_info();
#if 0
      std::thread t([&]() {
        std::unordered_map<uint64_t, uint64_t> distribution;
        size_t old_fail = 0;
        size_t fails = 0;
        size_t loads = 0;
        log_info() << "Starting mod test";
        while (1) {
          auto table = s.read<RDMAObj<routing_table> >(
              reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
              info.first->routing_table.rkey);
          loads++;
          table.first.get();

          if (!table.second.first->valid()) {
            fails++;
          } else {
            distribution[fails - old_fail]++;
            old_fail = fails;
          }
          if ((loads & 0xffff) == 0) {
            log_info() << "loads: " << loads << " fails: " << fails
                       << " ratio: " << (float)fails / loads;
            print_distribution(distribution);
          }
        }
      });
      t.detach();
#endif
    } break;
    default:
      break;
    }
    break;
  default:
    assert(false);
  }
}
  
hydra::routing_table hydra::passive::table() const {
  auto table = s.read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
      info.first->routing_table.rkey);
  table.first.get();
  return table.second.first->get();
}

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  notification_predecessor m(pred);
  s.sendImmediate(m);
}

void hydra::passive::send(const msg& m) const {
  s.sendImmediate(m);
}

bool hydra::passive::has_id(const keyspace_t &id) const {
  auto table = s.read<RDMAObj<routing_table> >(
      reinterpret_cast<uintptr_t>(info.first->routing_table.addr),
      info.first->routing_table.rkey);
  table.first.get();
  auto t = table.second.first->get();
  return id.in(t.self().node.id, t.successor().node.id);
//  return hydra::interval({t.self().node.id, t.successor().node.id}).contains(id);
}

