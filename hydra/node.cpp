#include <thread>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <tuple>
#include <exception>

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

node::node(std::vector<std::string> ips, const std::string &port, uint32_t msg_buffers)
    : socket(ips, port, msg_buffers), heap(48U, size2Class, socket),
      local_heap(socket), 
      table_ptr(
          heap.malloc<LocalRDMAObj<hash_table_entry> >(initial_table_size)),
      dht(table_ptr.first.get(), 32U, initial_table_size),
      info(heap.malloc<LocalRDMAObj<node_info> >()),
      request_buffers(msg_buffers),
      buffers_mr(socket.register_memory(
          ibv_access::REMOTE_READ | ibv_access::LOCAL_WRITE, request_buffers)) {
#if 1
  for (int msg_index = 0; msg_index < msg_buffers; msg_index++) {
    post_recv(request_buffers.at(msg_index));
  }
#else
        table.table[0].interval.range.first = k;
#endif

  info([&](auto &rdma_obj) {
         (*rdma_obj.first)([&](auto &info) {
           info.table_size = 8;
           info.key_extents = *table_ptr.second;
      info.id = keyspace_t(
          hash((ips.front() + port).c_str(), ips.front().size() + port.size()));

           log_info() << "key extents mr " << info.key_extents;
           log_info() << "table extents" << info.routing_table;
         });
         log_trace() << "node_info mr = " << rdma_obj.second;
       }).get();

  socket.listen();
  //socket.accept();
//  hydra::client test(ip, port);
}

void node::post_recv(const request_t &request) {
  auto future = socket.recv_async(request, buffers_mr.get());
  messageThread.send([ = , &request, future = std::move(future) ]() mutable {
    auto qp = future.get();
    try {
      recv(request, qp);
    }
    catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
    catch (...) {
      std::cout << "caught unknown thingy." << std::endl;
      std::terminate();
    }
    post_recv(request);
  });
}

void node::recv(const request_t &request, const qp_t &qp) {
  auto message = capnp::FlatArrayMessageReader(request);
  auto dht_request = message.getRoot<protocol::DHTRequest>();

  switch (dht_request.which()) {
  case protocol::DHTRequest::PUT: {
    auto put = dht_request.getPut();
    if (put.isRemote()) {
      handle_add(put.getRemote(), qp);
    } else {
      handle_add(put.getInline(), qp);
    }

  } break;
  case protocol::DHTRequest::DEL: {
    auto del = dht_request.getDel();
    if (del.isRemote()) {
      handle_del(del.getRemote(), qp);
    } else {
      handle_del(del.getInline(), qp);
    }
  } break;
  case protocol::DHTRequest::INIT: {
    ::capnp::MallocMessageBuilder response;
    hydra::protocol::DHTResponse::Builder msg =
        response.initRoot<hydra::protocol::DHTResponse>();

    info([&](const auto &info) {
      auto mr = msg.initInit().initInfo();
      mr.setAddr(reinterpret_cast<uintptr_t>(info.second->addr));
      mr.setSize(info.second->length);
      mr.setRkey(info.second->rkey);
    });
    reply(qp, response);
  } break;
  case protocol::DHTRequest::NETWORK: {
    reply(qp, routing_table->init());
  } break;
  }
}

void node::join(const std::string& ip, const std::string& port) {
#if 0
  hydra::overlay::chord remote(ip, port);
  init_routing_table(remote);
  update_others();
#if 0
  notify_ulp();
#endif
#endif
}

#if 0

void node::init_routing_table(const hydra::overlay::chord& remote) {
  (*routing_table_.first)([&](auto &table) {
    table.successor().node = remote.successor_node(table.successor().start);

    auto pred = remote.predecessor_node(table.successor().start);

    table.predecessor().node = pred;

    //table.successor().predecessor = me;
    auto succ = table.successor().node;
    hydra::passive successor_node(succ.ip, succ.port);
    successor_node.send(predecessor_message(table.self().node));

    std::transform(std::begin(table) + 1, std::end(table), std::begin(table),
                   std::begin(table) + 1,
                   [&](auto && elem, auto && prev)->hydra::routing_entry {
      if (elem.start.in(table.self().node.id, prev.node.id - 1)) {
        elem.node = prev.node;
      } else {
        // n'.find_successor(elem.interval.start);
        //elem.node = successor(remote, elem.start).node;
        auto succ = remote.successor_node(elem.start);
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
    keyspace_t id_ =
        routing_table_.first->get().self().node.id -
        keyspace_t(static_cast<hydra::keyspace_t::value_type>(1 << i) + 1);
    auto re = routing_table_.first->get().preceding_node(id_);
    hydra::overlay::chord node(re.node.ip, re.node.port);
    auto p = node.predecessor_node(id_);
    // send message to p
    // send self().node and i+1
    if (p.id != routing_table_.first->get().self().node.id) {
      hydra::async([=]() {
        RDMAClientSocket socket(p.ip, p.port);
        socket.connect();
        socket.send(update_message(routing_table_.first->get().self().node, i));
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
          socket.send(update_message(s, i));
        });
      }
      log_info() << table;
    }
  });
}
#endif

void node::handle_add(const protocol::DHTRequest::Put::Inline::Reader &reader,
                      const qp_t &qp) {
  auto data = reader.getData();

  auto mem = heap.malloc<unsigned char>(reader.getSize());
  auto key = mem.first.get();
  
  memcpy(key, data.begin(), reader.getSize());

  auto success =
      handle_add(std::move(mem), reader.getSize(), reader.getKeySize());
  reply(qp, ack_message(success));
}

void node::handle_add(const protocol::DHTRequest::Put::Remote::Reader &reader,
                      const qp_t &qp) {
  auto kv_reader = reader.getKv();
  const size_t size = kv_reader.getSize();
  const size_t key_size = reader.getKeySize();

  auto mem = heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  auto mr = mem.second;

  assert(key);

  auto fut = socket(qp, [=, &kv_reader](rdma_cm_id *id) {
    return rdma_read_async__(id, key, size, mr, kv_reader.getAddr(),
                             kv_reader.getRkey());
  });
  hydra::then(std::move(fut), [ =, mem = std::move(mem) ](auto s__) mutable {
    s__.get();
    auto success = handle_add(std::move(mem), size, key_size);
    reply(qp, ack_message(success));
  });
}

bool node::handle_add(rdma_ptr<unsigned char> kv, const size_t size,
                      const size_t key_size) {
#if 0
  if (!routing_table_.first->get().has_id(
           hydra::keyspace_t(hash(kv.first.get(), key_size)))) {
    log_err() << "Not responsible for key " << hash(kv.first.get(), key_size);
    return false;
  }
#endif
#if PER_ENTRY_LOCKS
  hopscotch_server &hs = dht;
#else
  return dht(
      [ =, kv = std::move(kv) ](hopscotch_server & hs) mutable {
#endif

  auto e =
      std::make_tuple(std::move(kv.first), size, key_size, kv.second->rkey);

  // hs.check_consistency();
  auto ret = hs.add(e);
  // hs.check_consistency();
  if (ret == hydra::NEED_RESIZE) {
    hs.dump();
    std::cout << "key: " << std::get<0>(e).get();
    info([&](auto &rdma_obj) {
      size_t new_size = hs.next_size();
      log_info() << "Allocating " << new_size
                 << " entries: " << new_size * sizeof(hash_table_entry);
      auto new_table = heap.malloc<LocalRDMAObj<hash_table_entry> >(new_size);
      hs.resize(new_table.first.get(), new_size);
      hs.check_consistency();
      (*rdma_obj.first)([&](auto &info) {
        info.table_size = new_size;
        info.key_extents = *new_table.second;
      });
      std::swap(table_ptr, new_table);
    });
    std::terminate();
    ret = hs.add(e);
    hs.check_consistency();
    assert(ret != hydra::NEED_RESIZE);
    return ret == hydra::SUCCESS;
#if 0
    notification_resize m(table_ptr.second);
    notify_all(m).get();
#endif
  }
  return ret == hydra::SUCCESS;
});
}

void node::handle_del(const protocol::DHTRequest::Del::Inline::Reader &reader,
                      const qp_t &qp) const {
  auto data = reader.getKey();

  auto mem = heap.malloc<unsigned char>(reader.getSize());
  auto key = mem.first.get();
  
  memcpy(key, data.begin(), reader.getSize());

  auto ret =
      dht([ =, &reader, mem = std::move(mem) ](hopscotch_server & s) mutable {
                                                server_dht::key_type key =
                                                    std::make_pair(
                                                        mem.first.get(),
                                                        reader.getSize());
                                                s.check_consistency();
                                                auto ret = s.remove(key);
                                                s.check_consistency();
                                                return ret;
                                              });
  reply(qp, ack_message(ret == hydra::SUCCESS));
}


void node::handle_del(const protocol::DHTRequest::Del::Remote::Reader &reader,
                      const qp_t &qp) const {
  auto mr = reader.getKey();
  
  const size_t size = mr.getSize();

  auto mem = local_heap.malloc<unsigned char>(size);
  auto key = mem.first.get();
  auto local_mr = mem.second;

  auto fut = socket(qp, [=, &mr](rdma_cm_id *id) {
    return rdma_read_async__(id, key, size, local_mr, mr.getAddr(),
                             mr.getRkey());
  });
  hydra::then(std::move(fut),
              [ =, mem = std::move(mem) ](auto && future) mutable {
    future.get();
    auto ret = dht([ =, mem = std::move(mem) ](hopscotch_server & s) mutable {
                                                server_dht::key_type key =
                                                    std::make_pair(
                                                        mem.first.get(), size);
                                                s.check_consistency();
                                                auto ret = s.remove(key);
                                                s.check_consistency();
                                                return ret;
                                              });
    reply(qp, ack_message(ret == hydra::SUCCESS));
  });
}

void node::reply(const qp_t &qp, ::capnp::MessageBuilder &reply) const {
  kj::Array<capnp::word> serialized = messageToFlatArray(reply);

  return socket(qp, [&](rdma_cm_id *id) {
    sendImmediate(id, serialized.begin(),
                  serialized.size() * sizeof(capnp::word));
  });
}

void node::reply(const qp_t &qp,
                 const ::kj::Array< ::capnp::word> &reply) const {
  return socket(qp, [&](rdma_cm_id *id) {
    sendImmediate(id, std::begin(reply), reply.size() * sizeof(capnp::word));
  });
}

double node::load() const {
#if PER_ENTRY_LOCKS
  return dht.load_factor();
#else
  return dht([](const hopscotch_server &s) { return s.load_factor(); });
#endif
}

void node::dump() const {
#if PER_ENTRY_LOCKS
#else
  dht([](const auto &s) { s.dump(); });
#endif
}
}

