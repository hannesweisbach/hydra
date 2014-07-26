#pragma once

#include <string>
#include <memory>
#include <vector>

#include "rdma/RDMAClientSocket.h"
#include "hydra/network.h"

namespace hydra {
namespace overlay {
namespace chord {

/* we have an identifier space of 2**128.
 * chord says we need a routing table of size 128.
 * I additionally store my predecessor and myself in the table.
 * [0] is my predecessor
 * [1] is myself
 * [2] is my successor
 * and so forth
 * TODO: put predecessor in slot 128., so the layout of the array corresponds to
 * chord.
 */
constexpr size_t routingtable_size =
    std::numeric_limits<keyspace_t::value_type>::digits + 2;
  
//decltype(heap.malloc<LocalRDMAObj<routing_table>>()) routing_table_;
#if 0
  /* call when joining the network - already running node ip */
  void init_routing_table(const hydra::overlay::chord& remote);
  void update_others() const;
  void update_routing_table(const hydra::node_id &e, const size_t i);
  void update_predecessor(const hydra::overlay::node_id &pred) const;

  
  (*routing_table_.first)([&](auto &table) {
    new (&table) hydra::routing_table(ips.front(), port);
    log_info() << table;
  });

  log_info() << "valid: " << routing_table_.first->valid();

void hydra::passive::update_predecessor(const hydra::node_id &pred) const {
  // no reply.
  send(predecessor_message(pred));
}

#endif

struct routing_table : public hydra::overlay::routing_table {
  kj::Array<capnp::word> init() const override;
  kj::Array<capnp::word> join(const std::string &host,
                              const std::string &port) override;

  static const size_t predecessor_index = 0;
  static const size_t self_index = 1;
  static const size_t successor_index = 2;

  std::array<routing_entry, routingtable_size> table;

  static_assert(routingtable_size > successor_index,
                "Table is not large enough");

  auto begin() const { return &successor(); }
  auto begin() { return &successor(); }
  auto end() const { return table.end(); }
  auto end() { return table.end(); }

  routing_table() = default;
  routing_table(const std::string &ip, const std::string &port,
                const keyspace_t &id) {
    table[predecessor_index] = routing_entry(ip, port, id);
    table[self_index] = routing_entry(ip, port, id);
    keyspace_t::value_type k = 0;
    for (auto &&entry : *this) {
      entry = routing_entry(ip, port, id, keyspace_t(k++));
    }
  }
  routing_table(const std::string &ip, const std::string &port)
      : routing_table(ip, port, keyspace_t(static_cast<keyspace_t::value_type>(
                                    hash(ip)))) {}
  const routing_entry preceding_node(const keyspace_t &id) const {
#if 0
    log_info() << std::hex << "Checking (" << (unsigned)self().node.id << " "
               << (unsigned)id << ") contains ";
#endif
    auto it =
        std::find_if(table.rbegin(), table.rend() + 2, [=](const auto &node) {
// return node.interval.contains(id);
#if 0
      log_info() << std::hex <<  "  " << (unsigned)node.node.id;
#endif
#if 1
          // gcc fails to call this->self(), so call it explicitly
          return node.node.id.in(this->self().node.id + 1_ID, id - 1_ID);
#else
          return interval(
              { static_cast<keyspace_t>(), static_cast<keyspace_t>(id - 1) })
              .contains(node.node.id);
#endif
        });
    assert(it != table.rend() + 2);

    return *it;
  }

  bool has_id(const keyspace_t &id) const {
    return id.in(predecessor().node.id + 1_ID, self().node.id);
  }

  routing_entry &operator[](const size_t i) {
    return table[i + successor_index];
  }
  const routing_entry &operator[](const size_t i) const {
    return table[i + successor_index];
  }

  const routing_entry &predecessor() const { return table[predecessor_index]; }
  routing_entry &predecessor() { return table[predecessor_index]; }
  const routing_entry &self() const { return table[self_index]; }
  routing_entry &self() { return table[self_index]; }
  const routing_entry &successor() const { return table[successor_index]; }
  routing_entry &successor() { return table[successor_index]; }
};

class chord : public virtual RDMAClientSocket, public network {
public:
  chord(const std::string &host, const std::string &port);
  chord(const std::string &host, const std::string &port, const uint64_t,
        const size_t, const uint32_t);
  ~chord();
  node_id predecessor_node(const keyspace_t &id) const;
  node_id successor_node(const keyspace_t &id) const;
  node_id self() const;

private:
  passive &successor(const keyspace_t &id) override;

  std::vector<network::node> cache;
  routing_table load_table() const;
  routing_table find_table(const keyspace_t &) const;
  std::unique_ptr<RDMAObj<routing_table> > table;
  mr_t local_table_mr;
  mr table_mr;
};

std::ostream &operator<<(std::ostream &s, const routing_table &t);
kj::Array<capnp::word> predecessor_message(const node_id &);
kj::Array<capnp::word> update_message(const node_id &, const size_t &);
kj::Array<capnp::word>
chord_response(const rdma_ptr<LocalRDMAObj<routing_table> > &);

}
}
}

