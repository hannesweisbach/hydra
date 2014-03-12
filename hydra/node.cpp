#include <thread>
#include <chrono>
#include <algorithm>

#include "node.h"
#include "util/concurrent.h"
#include "util/Logger.h"

namespace hydra {

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

node::node(const std::string &ip, const std::string &port, uint32_t msg_buffers)
    : socket(ip, port, msg_buffers), heap(48U, size2Class, socket),
      local_heap(socket),
      table_ptr(heap.malloc<key_entry>(8)),
      dht(table_ptr.first.get(), 12U, 8U),
      msg_buffer(local_heap.malloc<request>(msg_buffers)),
      info(heap.malloc<node_info>()) {
  for (size_t i = 0; i < msg_buffers; i++) {
    post_recv(msg_buffer.first.get()[i], msg_buffer.second);
  }

  info.first->table_size = 8;
  info.first->key_extents = *table_ptr.second;
  info.first->id = hash((ip + port).c_str(), ip.size() + port.size());
  log_info() << "key extents mr " << info.first->key_extents;

  log_trace() << "node_info mr = " << info.second;

  socket.listen();
  accept();
}

void node::connect(const std::string &host, const std::string &ip) {
  RDMAClientSocket s(host, ip);
  
  msg m;
  auto mr = s.mapMemory(&m);

  log_debug() << "msg is " << m;

  auto future = s.recv_async(m, mr);
  s.connect();

  future.get();
}
void node::accept() {
  auto f = socket.accept();
  //TODO: could also be placed into a single queue b/c of guaranteed order
#if 0
  hydra::then(std::move(f), [=](std::future<RDMAServerSocket::client_t> future) {
#else
  acceptThread.send([=,future=std::move(f)]()mutable {
#endif
  RDMAServerSocket::client_t id = future.get();
  rdma_cm_id * id_ = id.get();

  clients([id1 = std::move(id)](auto & clients) mutable {
            clients.emplace(id1->qp->qp_num, std::move(id1));
          }).get();

  log_info() << "ID: " << id_ << " " << (void *)id_;
  notification_init m(nullptr, info.second);
  log_hexdump(m);
  log_info() << m;
  {
    // ugly, but for now, I don't have a better idea.
    /* hold off until resizing done and data consistent */
    std::lock_guard<std::mutex> l(resize_mutex);
    if (rdma_post_send(id_, nullptr, &m, sizeof(m), nullptr, IBV_SEND_INLINE))
      log_err() << "rdma_post_send(): " << strerror(errno);
  }
  accept();
});
}

std::future<void> node::notify_all(const msg &m) {
  return clients([=](auto &clients) {
    for (auto &client : clients) {
      log_debug() << "Sending " << (void *)client.second.get() << " " << m;
      sendImmediate(client.second.get(), m);
    }
  });
}

void node::post_recv(const request &m, const ibv_mr *mr) {
  auto future = socket.recv_async(m, mr);
  messageThread.send([=,&m, future=std::move(future)]()mutable{
  auto qp = future.get();
  recv(m, qp);
  post_recv(m, mr);
});
}

void node::recv(const request &req, const qp_t &qp) {
  log_info() << req;
  assert(req.type() == msg::type::request);

  switch (req.subtype()) {
  case msg::subtype::put:
    handle_add(static_cast<const put_request &>(req), qp);
    break;
  case msg::subtype::del:
    handle_del(static_cast<const remove_request &>(req), qp);
    break;
  case msg::subtype::disconnect: {
    rdma_cm_id *id = find_id(qp).get();
    log_debug() << "Disconnecting " << (void *)id;
    rdma_disconnect(id);
    clients([=](auto &clients) { clients.erase(qp); });
  } break;
  }
}

void node::send(const uint64_t id) {
  log_trace() << "signalled send with id " << id << " completed.";
}

void node::handle_add(const put_request &msg, const qp_t &qp) {
  log_info() << msg;
  const size_t key_size = msg.key().size;
  const size_t val_size = msg.value().size;
  const size_t size = key_size + val_size;

  auto mem = heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  auto value = key + key_size;
  log_info() << mem.second;

  rdma_cm_id * id = find_id(qp).get();

  rdma_read_async__(id, value, val_size, mem.second, msg.value().addr,
                    msg.value().rkey).get();
  auto fut = rdma_read_async__(id, key, key_size, mem.second, msg.key().addr,
                               msg.key().rkey);
  hydra::then(
      std::move(fut),
      [ =, r = std::move(mem) ](auto s__) mutable {
        s__.get();
        dht([ =, r1 = std::move(r) ](hopscotch_server & hs) mutable {
                                      server_dht::resource_entry e(
                                          std::move(r1.first), r1.second->rkey,
                                          size, key_size);
                                      hs.check_consistency();
                                      auto ret = hs.add(std::move(e));
                                      hs.check_consistency();
                                      if (ret == hydra::NEED_RESIZE) {
                                        {
                                          std::lock_guard<std::mutex> l(
                                              resize_mutex);
                                          size_t new_size = hs.next_size();
                                          auto new_table =
                                              heap.malloc<key_entry>(new_size);
                                          hs.resize(new_table.first.get(),
                                                    new_size);
                                          hs.check_consistency();
                                          log_info() << info.first->table_size
                                                     << " "
                                                     << info.first->key_extents;
                                          info.first->table_size = new_size;
                                          info.first->key_extents =
                                              *new_table.second;
                                          log_info() << info.first->table_size << " "
                                                     << info.first->key_extents;
                                          std::swap(table_ptr, new_table);
                                        }
                                        notification_resize m(table_ptr.second);
                                        notify_all(m).get();
                                        ret = hs.add(std::move(e));
                                        hs.check_consistency();
                                      }
                                      // TODO: ack/nack according to return
                                      // value
                                      ack(qp, put_response(msg, true));
                                    });
      });
}

void node::handle_del(const remove_request &msg, const qp_t &qp) {
  log_info() << msg;

  const size_t size = msg.key().size;

  auto mem = local_heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  log_info() << mem.second;

  rdma_cm_id *id = find_id(qp).get();

  auto fut = rdma_read_async__(id, key, size, mem.second, msg.key().addr,
                               msg.key().rkey);
  hydra::then(std::move(fut), [ =, r = std::move(mem) ](auto s) mutable {
    s.get();
    dht([ =, r1 = std::move(r) ](hopscotch_server & s) mutable {
                                  server_dht::key_type key =
                                      std::make_pair(r1.first.get(), size);
                                  s.check_consistency();
                                  s.remove(key);
                                  s.check_consistency();
                                  // s.dump();
                                  // TODO: ack/nack according to return
                                  // value
                                  ack(qp, remove_response(msg, true));
                                });
  });
}

std::future<rdma_cm_id *> node::find_id(const qp_t &qp) const {
  return clients([=](auto & clients)->rdma_cm_id * {
    auto client = clients.find(qp);
    if (client != std::end(clients))
      return client->second.get();
    else
      return nullptr;
  });
}

void node::ack(const qp_t &qp, const response &r) const {
  // TODO: .then()
  rdma_cm_id *id = find_id(qp).get();
  assert(id != nullptr);
  sendImmediate(id, r);
}
}

