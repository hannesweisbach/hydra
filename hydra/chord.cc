#include "hydra/chord.h"
#include "hydra/types.h"
#include "hydra/client.h"
#include "hydra/protocol/message.h"

#include "hydra/RDMAObj.h"
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "dht.capnp.h"

namespace hydra {
namespace overlay {
namespace chord {

routing_entry preceding_node(const std::vector<entry_t> &table,
                             const keyspace_t &id) {
  auto self_id = table[routing_table::self_index].get().node.id;
  auto it =
      std::find_if(table.rbegin(), table.rend() + 2, [=](const auto &node) {
        return node.get().node.id.in(self_id + 1_ID, id - 1_ID);
      });
  assert(it != table.rend() + 2);

  return it->get();
}

chord::chord(const std::string &host, const std::string &port)
    : RDMAClientSocket(host, port) {
  kj::FixedArray<capnp::word, 7> response;
  auto mr = register_memory(ibv_access::MSG, response);

  auto future = recv_async(response, mr.get());

  send(network_request());
  future.get();

  auto message = capnp::FlatArrayMessageReader(response);
  auto reader = message.getRoot<protocol::DHTResponse>();
  assert(reader.which() == hydra::protocol::DHTResponse::NETWORK);
  auto network = reader.getNetwork();
  assert(network.getType() == hydra::protocol::DHTResponse::NetworkType::CHORD);
  auto t = network.getTable();
  table_mr.addr = t.getAddr();
  table_mr.size = t.getSize();
  table_mr.rkey = t.getRkey();

  local_table = std::vector<entry_t>(network.getSize());
  local_table_mr = register_memory(ibv_access::MSG, local_table);
}

chord::chord(const std::string &host, const std::string &port,
             const uint64_t addr, const uint32_t rkey, const uint16_t entries)
    : RDMAClientSocket(host, port), local_table(entries),
      local_table_mr(register_memory(ibv_access::MSG, local_table)) {
  table_mr.addr = addr;
  table_mr.rkey = rkey;
  load_table();
}

chord::~chord() {}

std::vector<entry_t> chord::load_table() {
  uint64_t addr = table_mr.addr;
  for (auto &&entry : local_table) {
    hydra::rdma::load(*this, entry, local_table_mr.get(), addr, table_mr.rkey);
    addr += sizeof(entry_t);
  }
  return local_table;
}

std::vector<entry_t> chord::find_table(const keyspace_t &id) {
  using namespace hydra::literals;

  auto table = load_table();
  auto start = table[routing_table::self_index].get().node.id + 1_ID;
  auto end = table[routing_table::successor_index].get().node.id;
  
  while (!id.in(start, end)) {
    auto re = preceding_node(table, id).node;
    chord node(re.ip, re.port);

    table = node.load_table();
    start = table[routing_table::self_index].get().node.id + 1_ID;
    end = table[routing_table::successor_index].get().node.id;
  }
  return table;
}

node_id chord::predecessor_node(const keyspace_t &id) {
  return find_table(id)[routing_table::self_index].get().node;
}

node_id chord::successor_node(const keyspace_t &id) {
  return find_table(id)[routing_table::successor_index].get().node;
}

node_id chord::self() {
  auto table = load_table();
  return table[routing_table::self_index].get().node;
}

passive &chord::successor(const keyspace_t &id) {
  auto table = find_table(id);
  const auto &self = table[routing_table::self_index].get();
  auto start = table[routing_table::predecessor_index].get().node.id + 1_ID;
  auto end = self.node.id;
  if (cache.empty()) {
    cache.emplace_back(start, end, self.node.ip, self.node.port);
  } else {
    cache[0] = network::node(start, end, self.node.ip, self.node.port);
  }
  return cache[0];
}

static kj::Array<capnp::word> predecessor_message(const std::string &host,
                                                  const std::string &port);

/* we have an identifier space of 2**k. chord says we need a routing table of
 * size k. I additionally store my predecessor and myself in the table, thus
 * size is k + 2:
 * [0] is my predecessor
 * [1] is myself
 * [2] is my successor
 * and so forth
 * TODO: put predecessor in slot 128., so the layout of the array corresponds to
 * chord.
 */

routing_table::routing_table(RDMAServerSocket &server, const std::string &ip,
                             const std::string &port)
    : hydra::overlay::routing_table(ip, port) {
  const auto table_size = std::numeric_limits<keyspace_t::value_type>::digits;
  static_assert(table_size > successor_index, "Keyspace is too small");

  auto id = keyspace_t(static_cast<keyspace_t::value_type>(hash(local_host)));

  table.emplace_back(local_host, local_port, id);
  table.emplace_back(local_host, local_port, id);

  keyspace_t k = 0_ID;
  std::generate_n(std::back_inserter(table), table_size,
                  [&]() { return entry_t(local_host, local_port, id, k++); });

  table_mr = server.register_memory(ibv_access::READ, table);

  for (const auto &e : table)
    std::cout << e.get() << std::endl;
}

kj::Array<capnp::word> routing_table::init() const {
  ::capnp::MallocMessageBuilder message;
  auto msg = message.initRoot<hydra::protocol::DHTResponse>();

  auto network = msg.initNetwork();
  network.setType(hydra::protocol::DHTResponse::NetworkType::CHORD);
  network.setSize(static_cast<uint16_t>(table.size()));
  auto remote = network.initTable();
  remote.setAddr(reinterpret_cast<uintptr_t>(table_mr->addr));
  remote.setSize(static_cast<uint32_t>(table_mr->length));
  remote.setRkey(table_mr->rkey);
  return messageToFlatArray(message);
}

kj::Array<capnp::word> routing_table::process_join(const std::string &host,
                                                   const std::string &port) {
  return kj::Array<capnp::word>();
}

void routing_table::update(const std::string &host, const std::string &port,
                           const keyspace_t &id, const size_t index) {
  if (id.in(table[self_index].get().node.id,
            table[index].get().node.id - 1_ID)) {
    table[index]([&](auto &&entry) {
      entry = routing_entry(host, port, id, entry.node.id);
    });
    auto pred = table[predecessor_index].get().node;
    if (pred.id != id) {
      // send message to p
      // p.update_finger_table(id, i + 1);
      hydra::async([=]()->void {
        RDMAClientSocket socket(pred.ip, pred.port);
        socket.connect();
        socket.send(update_message(host, port, id, index));
      });
    }

    for (const auto &e : table)
      std::cout << e.get() << std::endl;
  }
}

std::pair<keyspace_t, keyspace_t> routing_table::join(const std::string &host,
                                                      const std::string &port) {
  hydra::overlay::chord::chord remote(host, port);

  // TODO: join message to get an interval

  auto successor_id = table[successor_index].get().start;

  table[successor_index]([&](auto &&entry) {
    entry.node = remote.successor_node(successor_id);
  });

  table[predecessor_index]([&](auto &&entry) {
    entry.node = remote.predecessor_node(successor_id);
  });

  // table.successor().predecessor = me;
  const auto &successor = table[successor_index].get().node;
  hydra::passive successor_node(successor.ip, successor.port);
  successor_node.send(predecessor_message(local_host, local_port));

  std::transform(std::begin(table) + 1, std::end(table), std::begin(table),
                 std::begin(table) + 1,
                 [&](const auto & elem, const auto & prev)->entry_t {
    auto result = elem;
    const auto self_id = table[self_index].get().node.id;
    if (elem.get().start.in(self_id, prev.get().node.id - 1_ID)) {
      result([&](auto &&entry) { entry.node = prev.get().node; });
    } else {
      // n'.find_successor(elem.interval.start);
      // elem.node = successor(remote, elem.start).node;
      auto succ = remote.successor_node(elem.get().start);
      if (!self_id.in(elem.get().start, succ.id))
        result([&](auto &&entry) { entry.node = succ; });
    }
    return result;
  });

  for (const auto &e : table)
    std::cout << e.get() << std::endl;

  //update others
  const keyspace_t max =
      keyspace_t(std::numeric_limits<keyspace_t::value_type>::digits);
  const auto self_id = table[self_index].get().node.id;

  for (keyspace_t i = 0_ID; i < max; i++) {
    keyspace_t id_ = self_id - ((1_ID << i) + 1_ID);
    auto re = preceding_node(table, id_);
    hydra::overlay::chord::chord node(re.node.ip, re.node.port);
    auto p = node.predecessor_node(id_);
    // send message to p
    // send self().node and i+1
    if (p.id != self_id) {
      hydra::async([=]() {
        RDMAClientSocket socket(p.ip, p.port);
        socket.connect();
        socket.send(update_message(local_host, local_port, self_id, i));
      });
    }
    // p.update_finger_table(id, i + 1);
  }

  return { table[self_index].get().start, table[self_index].get().node.id };
}

kj::Array<capnp::word> predecessor_message(const std::string &host,
                                           const std::string &port) {
  ::capnp::MallocMessageBuilder response;

  auto n = response.initRoot<hydra::protocol::DHTRequest>()
               .initOverlay()
               .initPredecessor()
               .initNode();
  init_node(host, port, n);
  return messageToFlatArray(response);
}

std::ostream &operator<<(std::ostream &s, const routing_table &t) {
  s << "routing_table " << t.table.size() << std::endl;
  {
    indent_guard guard(s);
    s << indent << "pred: " << t.table[routing_table::predecessor_index].get()
      << std::endl;
    s << indent << "self: " << t.table[routing_table::self_index].get()
      << std::endl;
    size_t i = 1;
    for (auto &&e : t.table) {
      s << indent << "[" << std::setw(3) << i++ << "] " << e.get() << " "
        << std::endl;
    }
  }
  return s;
}

}
}
}

