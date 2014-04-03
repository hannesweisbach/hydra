#include <iostream>
#include <map>
#include <string>
#include <ostream>
#include <vector>
#include <algorithm>
#include <iterator>

#include "hydra/types.h"

using namespace hydra::literals;

static std::map<hydra::keyspace_t, hydra::routing_table> network;

bool node_exists(const hydra::keyspace_t &id) {
  return network.find(id) != std::end(network);
}

hydra::keyspace_t successor(const hydra::keyspace_t &id) {
  assert(!network.empty());
  auto succ = std::find_if(std::begin(network), std::end(network),
                           [&](const auto &elem) {
    assert(elem.first == elem.second.self().node.id);
    return elem.second.self().node.id >= id;
  });
  if (succ == std::end(network))
    succ = std::begin(network);

  return succ->first;
}

hydra::keyspace_t predecessor(const hydra::keyspace_t &id) {
  assert(!network.empty());

  auto succ =
      std::find_if(network.crbegin(), network.crend(), [&](const auto &elem) {
        assert(elem.first == elem.second.self().node.id);
        return elem.second.self().node.id < id;
      });

  if (succ == network.crend())
    succ = network.crbegin();

  return succ->first;
}

hydra::routing_entry predecessor(const hydra::routing_table &table,
                                 const hydra::keyspace_t &id) {
  hydra::routing_table t = table;
  while (!id.in(t.self().node.id + 1, t.successor().node.id)) {
    auto preceding_node = t.preceding_node(id).node.id;
    t = network[preceding_node];
  }

  assert(t.self().node.id == predecessor(id));

  return t.self();
}

hydra::routing_entry successor(const hydra::routing_table &table,
                               const hydra::keyspace_t &id) {
  auto pred = predecessor(table, id);
  auto succ = network[pred.node.id].successor();

  assert(succ.node.id == successor(id));

  return succ;
}

void join(hydra::routing_table &n, const hydra::routing_table &old) {
  n.successor().node = successor(old, n.self().node.id).node;
}

void notify(hydra::routing_table &node, const hydra::node_id &possible_pred) {
  if (possible_pred.id.in(node.predecessor().node.id + 1,
                          node.self().node.id - 1)) {
    node.predecessor().node = possible_pred;
  }
}

void stabilize(hydra::routing_table &node) {
  auto x = network[node.successor().node.id].predecessor().node;
  if (x.id.in(node.self().node.id + 1, node.successor().node.id)) {
    node.successor().node = x;
  }
  notify(network[node.successor().node.id], node.self().node);
}

void fix_fingers(hydra::routing_table &node) {
  for (auto &&finger : node) {
    finger.node = successor(node, finger.start).node;
  }
}

void stabilize_all(const hydra::keyspace_t &key) {
  stabilize(network[key]);
  stabilize(network[predecessor(key)]);

#if 1
  for (auto &&node : network) {
    fix_fingers(node.second);
  }
#else
  fix_fingers(network[key]);
  log_info() << network[key];
  for (size_t i = 0; i < std::numeric_limits<hydra::keyspace_t>::digits; i++) {
    /* TODO: need to recursively update others. */
    fix_fingers(network[predecessor(static_cast<hydra::keyspace_t>(
        key + std::numeric_limits<hydra::keyspace_t>::max() - (1 << i) - 1))]);
    log_info() << hydra::hex(key +
                             std::numeric_limits<hydra::keyspace_t>::max() -
                             (1 << i) - 1) << " "
               << hydra::hex(predecessor(
                      static_cast<hydra::keyspace_t>(key - (1 << i) - 1)));
    //    log_info() << network[predecessor(static_cast<hydra::keyspace_t>(key -
    // (1 << i) - 1))];
  }
#endif
}

void dump() {
  size_t i = 0;
  for (auto it = std::begin(network); it != std::end(network); ++it, i++) {
    auto next = it;
    if (++next == std::end(network))
      next = std::begin(network);
    log_info() << std::setw(3) << i << " " << it->second.self() << " "
               << it->second.successor() << " next: " << next->second.self();
    //    log_info() << it->second.self() << " " << it->second;
  }
}

void check_successors() {
#if 0
  size_t i = 0;
  for (auto it = std::begin(network); it != std::end(network); ++it, i++) {
    auto next = it;
    if (++next == std::end(network))
      next = std::begin(network);
    log_info() << std::setw(3) << i << " " << it->second.predecessor() << " | "
               << it->second.self() << " | " << it->second.successor()
               << " next: " << next->second.self();
    //log_info() << it->second.self() << " " << it->second;
  }
#endif

  if (network.size() == 1) {
    auto it = std::begin(network);
    assert(it->first == it->second.self().node.id);
    assert(it->second.self().node.id == it->second.successor().node.id);
    assert(it->second.self().node.id == it->second.predecessor().node.id);

    return;
  }

  for (auto it = std::begin(network); it != std::end(network); ++it) {
    assert(it->first == it->second.self().node.id);

    auto next = it;
    if (++next == std::end(network)) {
      next = std::begin(network);
      assert(it->second.self().node.id > next->second.self().node.id);
    } else {
      assert(it->second.self().node.id < next->second.self().node.id);
    }

    auto prev = it;
    if (prev == std::begin(network)) {
      prev = std::end(network);
      --prev;
      assert(it->second.self().node.id < prev->second.self().node.id);
    } else {
      --prev;
      assert(it->second.self().node.id > prev->second.self().node.id);
    }

    assert(it->second.self().node.id != prev->second.self().node.id);
    assert(it->second.self().node.id != next->second.self().node.id);
    assert(it->second.successor().node.id == next->second.self().node.id);
    assert(it->second.predecessor().node.id == prev->second.self().node.id);

    for (const auto &finger : it->second) {
      assert(finger.node.id == successor(finger.start));
    }
  }
}

int main() {
  const std::string port("8042");
  const std::string seed_node = "1";

  network.emplace(hydra::hash(seed_node),
                  hydra::routing_table(seed_node, port));

  for (size_t i = 2; i < 0xffffffffe; i++) {
    std::ostringstream s;
    s << i;
    if (node_exists(hydra::hash(s.str()))) {
      continue;
    }

    log_info() << "Adding node " << s.str() << " " << network.size();

    hydra::routing_table new_table(s.str(), port);
    auto key = new_table.self().node.id;

    join(new_table, network[hydra::hash(seed_node)]);
    network.emplace(key, new_table);

    stabilize_all(key);

    check_successors();

    if (network.size() ==
        std::numeric_limits<hydra::keyspace_t::value_type>::max())
      break;
  }

  log_info() << "Checked with " << network.size() << " nodes";
}

