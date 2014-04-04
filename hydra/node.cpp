#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>

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
      local_heap(socket), table_ptr(heap.malloc<key_entry>(8)),
      dht(table_ptr.first.get(), 12U, 8U),
      msg_buffer(local_heap.malloc<msg>(msg_buffers)),
      info(heap.malloc<LocalRDMAObj<node_info> >()),
      routing_table(heap.malloc<LocalRDMAObj<hydra::routing_table> >()) {

  (*routing_table.first)([&](auto &table) {
    new (&table) struct routing_table(ip, port);
    log_info() << table;
  });

  log_info() << "valid: " << routing_table.first->valid();
#if 0
  std::thread t([&,n = hash(ip)]() {
    size_t k = 0;
    log_info() << "Starting mod loop";
    std::chrono::nanoseconds diff(0);
    while (1) {
      auto start = std::chrono::high_resolution_clock::now();
      (*routing_table.first)([&](auto &table) {
        size_t i = k % table.table.size();
#if 0
        table.table[0] = routing_entry(n, k);
#else
        table.table[0].interval.range.first = k;
#endif
      });
      auto end = std::chrono::high_resolution_clock::now();
      diff += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
      k++;
      if ((k & 0xfffff) == 0) {
        log_info() << "mod " << k << " " << diff.count() / 0xfffff;
        diff = std::chrono::nanoseconds(0);
      }
    }
  });
  t.detach();
#endif
  for (size_t i = 0; i < msg_buffers; i++) {
    post_recv(msg_buffer.first.get()[i], msg_buffer.second);
  }

  info([&](auto &rdma_obj) {
         (*rdma_obj.first)([&](auto &info) {
           info.table_size = 8;
           info.key_extents = *table_ptr.second;
           info.routing_table = *routing_table.second;
           info.id = static_cast<keyspace_t::value_type>(
               hash((ip + port).c_str(), ip.size() + port.size()));

           log_info() << "key extents mr " << info.key_extents;
           log_info() << "table extents" << info.routing_table;
         });
         log_trace() << "node_info mr = " << rdma_obj.second;
       }).get();

  socket.listen();
  accept();

//  hydra::client test(ip, port);
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

void node::post_recv(const msg &m, const ibv_mr *mr) {
  auto future = socket.recv_async(m, mr);
  messageThread.send([=,&m, future=std::move(future)]()mutable{
  auto qp = future.get();
  recv(m, qp);
  post_recv(m, mr);
});
}

void node::recv(const msg &req, const qp_t &qp) {
  log_info() << req;

  assert(req.type() == msg::type::request || req.type() == msg::type::notification);

  switch (req.subtype()) {
  case msg::subtype::put:
    handle_add(static_cast<const put_request &>(req), qp);
    break;
  case msg::subtype::del:
    handle_del(static_cast<const remove_request &>(req), qp);
    break;
  case msg::subtype::init: {
    auto request = static_cast<const init_request &>(req);
    info([&](const auto &info) {
           init_response m(request, info.second);
           log_info() << m;
           sendImmediate(find_id(qp).get(), m);
         }).get();
  } break;
  case msg::subtype::predecessor: {
    auto notification = static_cast<const notification_predecessor &>(req);
    (*routing_table.first)([&](auto &table) {
      table.predecessor().node = notification.predecessor();
    });
  } break;
  case msg::subtype::routing: {
    auto notification = static_cast<const notification_update &>(req);
    update_routing_table(notification.node(), notification.index());
  } break;
  case msg::subtype::disconnect: {
    rdma_cm_id *id = find_id(qp).get();
    ack(qp, disconnect_response(static_cast<const disconnect_request &>(req)));
    log_debug() << "Disconnecting " << (void *)id;
    rdma_disconnect(id);
    clients([=](auto &clients) { clients.erase(qp); });
  } break;
  }
}

void node::join(const std::string& ip, const std::string& port) {
  hydra::passive remote(ip, port);
  init_routing_table(remote);
  update_others();
#if 0
  notify_ulp();
#endif
}

void node::init_routing_table(const hydra::passive& remote) {
  (*routing_table.first)([&](auto &table) {
    table.successor().node = successor(remote, table.successor().start).node;

    auto pred1 = predecessor(remote, table.successor().start);
    hydra::passive tmp(pred1.node.ip, pred1.node.port);
    auto pred2 = tmp.table().predecessor();

    log_info() << pred1 << " " << pred2;

    table.predecessor() = pred1;

    //table.successor().predecessor = me;
    auto succ = table.successor().node;
    hydra::passive pred(succ.ip, succ.port);
    pred.send(notification_predecessor(table.self().node));

    std::transform(std::begin(table) + 1, std::end(table), std::begin(table),
                   std::begin(table) + 1,
                   [&](auto && elem, auto && prev)->hydra::routing_entry {
      if (elem.start.in(table.self().node.id, prev.node.id - 1)) {
        elem.node = prev.node;
      } else {
        // n'.find_successor(elem.interval.start);
        //elem.node = successor(remote, elem.start).node;
        auto succ = successor(remote, elem.start).node;
        if (!table.self().node.id.in(elem.start, succ.id))
          elem.node = succ;
      }
      return elem;
    });
    log_info() << table;
  });
}

hydra::routing_table node::find_table(const hydra::routing_table &start,
                                      const keyspace_t &id) const {
  hydra::routing_table table = start;
  while (!id.in(table.self().node.id + 1, table.successor().node.id)) {
    auto re = table.preceding_node(id).node;
    hydra::passive node(re.ip, re.port);
    table = node.table();
  }
  return table;
}

hydra::routing_entry node::predecessor(const hydra::routing_table &start,
                                       const keyspace_t &id) const {
  return find_table(start, id).self();
}

hydra::routing_entry node::successor(const hydra::routing_table &start,
                                     const keyspace_t &id) const {
  return find_table(start, id).successor();
}

hydra::routing_table node::find_table(const hydra::passive &remote,
                                      const keyspace_t &id) const {
  auto table = remote.table();
  log_debug() << "Looking for ID " << id;
  // TODO cache table
  while (!id.in(table.self().node.id + 1, table.successor().node.id)) {
    // call preceding node on rdma-read table;
    auto re = table.preceding_node(id).node;
    log_debug() << "ID " << id << " is not in " << table.self().node.id << " "
                << table.successor().node.id;
    log_debug() << "Checking node " << re;
    hydra::passive node(re.ip, re.port);
    table = node.table();
  }

  log_debug() << "Found ID " << id << " on node " << table.self();

  return table;
}

hydra::routing_entry node::predecessor(const hydra::passive &remote,
                                       const keyspace_t &id) const {
  return find_table(remote, id).self();
}

hydra::routing_entry node::successor(const hydra::passive &remote,
                                     const keyspace_t &id) const {
  return find_table(remote, id).successor();
}

/* here my own routing table is up and running */
void node::update_others() const {
  const size_t max = std::numeric_limits<hydra::keyspace_t::value_type>::digits;
  for (size_t i = 0; i < max; i++) {
    keyspace_t id_ = routing_table.first->get().self().node.id -
                     static_cast<hydra::keyspace_t::value_type>(1 << i) + 1;
    auto p = predecessor(routing_table.first->get(), id_).node;
    // send message to p
    // send self().node and i+1
    if (p.id != routing_table.first->get().self().node.id) {
      hydra::passive node(p.ip, p.port);
      node.send(notification_update(routing_table.first->get().self().node, i));
    }
    // p.update_finger_table(id, i + 1);
  }
}

//called upon reception of update message
void node::update_routing_table(const hydra::node_id &s, const size_t i) {
  (*routing_table.first)([&](auto &table) {
    if (s.id.in(table.self().node.id, table[i].node.id - 1)) {
      table[i].node = s;
      auto pred = table.predecessor().node;
      if (pred.id != s.id) {
        hydra::passive node(pred.ip, pred.port);
        node.send(notification_update(s, i));
        // send message to p
        // p.update_finger_table(id, i + 1);
      }
      log_info() << table;
    }
  });
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
                                        info([&](auto &rdma_obj) {
                                               size_t new_size = hs.next_size();
                                               auto new_table =
                                                   heap.malloc<key_entry>(
                                                       new_size);
                                               hs.resize(new_table.first.get(),
                                                         new_size);
                                               hs.check_consistency();
                                               (*rdma_obj.first)([&](
                                                   auto &info) {
                                                 log_info() << info.table_size
                                                            << " "
                                                            << info.key_extents;
                                                 info.table_size = new_size;
                                                 info.key_extents =
                                                     *new_table.second;
                                                 log_info() << info.table_size
                                                            << " "
                                                            << info.key_extents;
                                               });
                                               std::swap(table_ptr, new_table);
                                             }).get();
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
    else {
      std::ostringstream s;
      s << "rdma_cm_id* for qp " << qp << " not found." << std::endl;
      throw std::runtime_error(s.str());
    }
  });
}

void node::ack(const qp_t &qp, const response &r) const {
  // TODO: .then()
  rdma_cm_id *id = find_id(qp).get();
  assert(id != nullptr);
  sendImmediate(id, r);
}

}

