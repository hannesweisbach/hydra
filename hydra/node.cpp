#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>

#include "node.h"
#include "hydra/chord.h"

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
      local_heap(socket), table_ptr(heap.malloc<LocalRDMAObj<key_entry> >(8)),
      dht(table_ptr.first.get(), 12U, 8U),
      msg_buffer(local_heap.malloc<msg>(msg_buffers)),
      info(heap.malloc<LocalRDMAObj<node_info> >()),
      routing_table_(heap.malloc<LocalRDMAObj<hydra::routing_table> >()) {

  (*routing_table_.first)([&](auto &table) {
    new (&table) hydra::routing_table(ip, port);
    log_info() << table;
  });

  log_info() << "valid: " << routing_table_.first->valid();
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
           info.routing_table = *routing_table_.second;
           info.id = static_cast<keyspace_t::value_type>(
               hash((ip + port).c_str(), ip.size() + port.size()));

           log_info() << "key extents mr " << info.key_extents;
           log_info() << "table extents" << info.routing_table;
         });
         log_trace() << "node_info mr = " << rdma_obj.second;
       }).get();

  socket.listen();
  //socket.accept();
//  hydra::client test(ip, port);
}

std::future<void> node::notify_all(const msg &m) {
  return socket([=](rdma_cm_id *id) { sendImmediate(id, m); });
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

  assert(req.type() == msg::mtype::request || req.type() == msg::mtype::notification);

  switch (req.subtype()) {
  case msg::msubtype::put:
    handle_add(static_cast<const put_request &>(req), qp);
    break;
  case msg::msubtype::del:
    handle_del(static_cast<const remove_request &>(req), qp);
    break;
  case msg::msubtype::init: {
    auto request = static_cast<const init_request &>(req);
    info([&](const auto &info) {
           init_response m(request, info.second);
           log_info() << m;
           socket(qp, [=](rdma_cm_id *id) { sendImmediate(id, m); });
         }).get();
  } break;
  case msg::msubtype::predecessor: {
    auto notification = static_cast<const notification_predecessor &>(req);
    (*routing_table_.first)([&](auto &table) {
      table.predecessor().node = notification.predecessor();
    });
  } break;
  case msg::msubtype::routing: {
    auto notification = static_cast<const notification_update &>(req);
    update_routing_table(notification.node(), notification.index());
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
  (*routing_table_.first)([&](auto &table) {
    table.successor().node = chord::successor(remote.table(), table.successor().start);

    auto pred = chord::predecessor(remote.table(), table.successor().start);

    table.predecessor().node = pred;

    //table.successor().predecessor = me;
    auto succ = table.successor().node;
    hydra::passive successor_node(succ.ip, succ.port);
    successor_node.send(notification_predecessor(table.self().node));

    std::transform(std::begin(table) + 1, std::end(table), std::begin(table),
                   std::begin(table) + 1,
                   [&](auto && elem, auto && prev)->hydra::routing_entry {
      if (elem.start.in(table.self().node.id, prev.node.id - 1)) {
        elem.node = prev.node;
      } else {
        // n'.find_successor(elem.interval.start);
        //elem.node = successor(remote, elem.start).node;
        auto succ = chord::successor(remote.table(), elem.start);
        if (!table.self().node.id.in(elem.start, succ.id))
          elem.node = succ;
      }
      return elem;
    });
    log_info() << table;
  });
}

/* here my own routing table is up and running */
void node::update_others() const {
  const size_t max = std::numeric_limits<hydra::keyspace_t::value_type>::digits;
  for (size_t i = 0; i < max; i++) {
    keyspace_t id_ = routing_table_.first->get().self().node.id -
                     static_cast<hydra::keyspace_t::value_type>(1 << i) + 1;
    auto p = chord::predecessor(routing_table_.first->get(), id_);
    // send message to p
    // send self().node and i+1
    if (p.id != routing_table_.first->get().self().node.id) {
      hydra::async([=]() {
        RDMAClientSocket socket(p.ip, p.port);
        socket.connect();
        socket.sendImmediate(
            notification_update(routing_table_.first->get().self().node, i));
      });
    }
    // p.update_finger_table(id, i + 1);
  }
}

//called upon reception of update message
void node::update_routing_table(const hydra::node_id &s, const size_t i) {
  (*routing_table_.first)([=](auto &table) {
    if (s.id.in(table.self().node.id, table[i].node.id - 1)) {
      table[i].node = s;
      auto pred = table.predecessor().node;
      if (pred.id != s.id) {
        // send message to p
        // p.update_finger_table(id, i + 1);
        hydra::async([=]() {
          RDMAClientSocket socket(pred.ip, pred.port);
          socket.connect();
          socket.sendImmediate(notification_update(s, i));
        });
      }
      log_info() << table;
    }
  });
}

void node::send(const uint64_t id) {
  log_trace() << "signalled send with id " << id << " completed.";
}

void node::handle_add(const put_request &msg, const qp_t &qp) {
  const size_t key_size = msg.key().size;
  const size_t val_size = msg.value().size;
  const size_t size = key_size + val_size;

  auto mem = heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  auto value = key + key_size;
  auto mr = mem.second;

  auto fut = socket(qp, [=](rdma_cm_id *id) {
    rdma_read_async__(id, value, val_size, mr, msg.value().addr,
                      msg.value().rkey).get();
    return rdma_read_async__(id, key, key_size, mr, msg.key().addr,
                             msg.key().rkey);
  });
  hydra::then(
      std::move(fut),
      [ =, r = std::move(mem) ](auto s__) mutable {
        s__.get();
        if (!routing_table_.first->get().has_id(
                 static_cast<hydra::keyspace_t::value_type>(
                     hash(r.first.get(), key_size)))) {
          log_err() << "Not responsible for key "
                    << hash(r.first.get(), key_size);
          this->ack(qp, put_response(msg, false));
          return;
        }
        dht([ =, r1 = std::move(r) ](hopscotch_server & hs) mutable {
                                      server_dht::resource_entry e(
                                          std::move(r1.first), r1.second->rkey,
                                          size, key_size);
#ifndef NDEBUG
                                      hs.check_consistency();
#endif
                                      auto ret = hs.add(std::move(e));
#ifndef NDEBUG
                                      hs.check_consistency();
#endif
                                      if (ret == hydra::NEED_RESIZE) {
                                        info([&](auto &rdma_obj) {
                                               size_t new_size = hs.next_size();
                                               auto new_table = heap.malloc<
                                                   LocalRDMAObj<key_entry> >(
                                                   new_size);
                                               hs.resize(new_table.first.get(),
                                                         new_size);
#ifndef NDEBUG
                                               hs.check_consistency();
#endif
                                               (*rdma_obj.first)([&](
                                                   auto &info) {
                                                 info.table_size = new_size;
                                                 info.key_extents =
                                                     *new_table.second;
                                               });
                                               std::swap(table_ptr, new_table);
                                             }).get();
                                        ret = hs.add(std::move(e));
#ifndef NDEBUG
                                        hs.check_consistency();
#endif
                                        this->ack(qp, put_response(msg, ret == hydra::SUCCESS));
#if 1
                                        return;
#else
                                        notification_resize m(table_ptr.second);
                                        notify_all(m).get();
#endif
                                      }
                                      // TODO: ack/nack according to return
                                      // value
                                      this->ack(qp, put_response(msg, ret == hydra::SUCCESS));
                                    });
      });
}

void node::handle_del(const remove_request &msg, const qp_t &qp) {
  const size_t size = msg.key().size;

  auto mem = local_heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  auto mr = mem.second;
  log_info() << mem.second;

  auto fut = socket(qp, [=](rdma_cm_id *id) {
    return rdma_read_async__(id, key, size, mr, msg.key().addr,
                             msg.key().rkey);
  });

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
                                  this->ack(qp, remove_response(msg, true));
                                });
  });
}

void node::ack(const qp_t &qp, const response &r) const {
  socket(qp, [=](rdma_cm_id *id) { sendImmediate(id, r); });
}
}

