#include <iostream>
#include <map>
#include <string>
#include <ostream>
#include <vector>
#include <algorithm>
#include <iterator>

#include "hydra/types.h"
#include "util/Logger.h"

static std::unordered_map<std::string, hydra::routing_table> network;

hydra::routing_entry predecessor(const hydra::routing_table &table,
                                 const hydra::keyspace_t &id) {
  hydra::routing_entry node = table.self();
  hydra::routing_table t = table;
  while (!id.in(t.self().node.id + 1, t.successor().node.id)) {
    auto preceding_node = t.preceding_node(id);
    t = network[preceding_node.node.id];
    node = t.self();
  }

  //assert(node.node.id == predecessor(id));

  return node;
}

hydra::routing_entry successor(const hydra::routing_table &table,
                               const hydra::keyspace_t &id) {
  auto pred = predecessor(table, id);
  auto succ = network[pred.node.id].successor();

  //assert(succ.node.id == successor(id));

  return succ;
}

void init_table(hydra::routing_table &n, const hydra::routing_table &n_) {
  const hydra::keyspace_t start = n[0].interval.start;
  log_info() << "I am " << n.self().node;
#endif

  n.successor().node = successor(n_, start).node;
#if 1
  log_info() << "My successor is " << n.successor().node;
#endif

  /* read predecessor-pointer from successor */
  n.predecessor().node = network[n.successor().node.id].predecessor().node;
#if 1
  log_info() << "My predecessor is " << n.predecessor().node;
#endif

/* write predecessor-pointer to successor */
#if 1
  log_info() << "My successors predecessor was "
             << network[n.successor().node.id].predecessor().node;
#endif
  network[n.successor().node.id].predecessor().node = n.self().node;
#if 1
  log_info() << "My successors predecessor is "
             << network[n.successor().node.id].predecessor().node;
#endif

#if 0
  auto pred = predecessor(n_, n.self().node.id);
  log_info() << "My pred's successor was " << network[pred.node.id].successor();

  network[pred.node.id].successor() = n.self();
  log_info() << "My pred's successor is " << network[pred.node.id].successor();
#endif

  hydra::keyspace_t id = n.self().node.id;
  network.emplace(n.self().node.id, n);
  hydra::routing_table& t = network[id];

  std::transform(std::begin(t) + 1, std::end(t), std::begin(t),
                 std::begin(t) + 1,
                 [&](auto && elem, auto && prev)->hydra::routing_entry {
    if (hydra::interval({ n.self().node.id,
                          static_cast<hydra::keyspace_t>(prev.node.id - 1) })
            .contains(elem.interval.start)) {
      log_info() << std::hex << (unsigned)elem.interval.start << " in ["
                 << (unsigned)n.self().node.id << ", " << (unsigned)prev.node.id
                 << ")";
      elem.node = prev.node;
    } else {
      log_info() << std::hex << (unsigned)elem.interval.start << " !in ["
                 << (unsigned)n.self().node.id << ", " << (unsigned)prev.node.id
                 << ")";
      // n'.find_successor(elem.interval.start);
      elem.node = successor(n_, elem.interval.start).node;
      log_info() << std::hex << (unsigned)elem.interval.start << " "
                 << elem.node;
    }
    return elem;
  });

#if 1
  log_info() << "New routing table of  " << network[id].self() << " " << network[id];
#endif
}

void update_table(hydra::routing_table &table, const hydra::routing_entry &s,
                  size_t &i) {
  indent_guard guard(Logger::underlying_stream);

  if (s.node.id.in(table.self().node.id, table[i].node.id - 1)) {
#if 1
    log_info() << indent << s.node.id << "  in [" << table.self().node.id
               << ", " << table[i].node.id << ") " << i;
#endif
#if 1
    log_info() << indent << "Updating entry " << i << " of " << table.self()
               << " from " << table[i].node << " to " << s.node;
#endif
    table[i].node = s.node;
    auto pred = table.predecessor();
    if (pred.node.id != s.node.id)
    {
#if 1
      log_info() << indent << "Notifying " << pred << " to update " << s << " "
                 << i;
#endif
      update_table(network[pred.node.id], s, i);
    }
  } else {
#if 1
    log_info() << indent << s.node.id << " !in [" << table.self().node.id
               << ", " << table[i].node.id << ") " << i;
#endif
  }
}

void update_others(const hydra::routing_table &table) {
  for (size_t k = 0; k < std::numeric_limits<hydra::keyspace_t>::digits; k++) {
    hydra::keyspace_t key =
        table.self().node.id - static_cast<hydra::keyspace_t>((1 << k) - 1);

    auto pred = predecessor(table, key);
#if 1
    log_info() << "Update pred(" << key << ") " << pred.node;
#endif
    if (pred.node.id != table.self().node.id)
      update_table(network[pred.node.id], table.self(), k);
  }
}

void dump() {
  size_t i = 0;
  for (auto it = std::begin(network); it != std::end(network); ++it, i++) {
    auto next = it;
    if (++next == std::end(network))
      next = std::begin(network);
    log_info() << std::setw(3) << i << " " << it->second.self() << " "
               << it->second.successor() << " next: " << next->second.self();
    log_info() << it->second.self() << " " << it->second;
  }
}

void check() {
  dump();

  for (auto it = std::begin(network); it != std::end(network); ++it) {
    assert(it->first == it->second.self().node.id);

    auto next = it;
    if (++next == std::end(network)) {
      next = std::begin(network);
      assert(it->second.self().node.id > next->second.self().node.id);
    } else {
      assert(it->second.self().node.id < next->second.self().node.id);
    }
    assert(it->second.self().node.id != next->second.self().node.id);
    assert(it->second.successor().node.id == next->second.self().node.id);
  }

  size_t i = 0;

  for (auto it = std::begin(network); it != std::end(network); ++it, i++) {
    const auto next = (++it == std::end(network)) ? std::begin(network) : it;
    auto prev = (--it == std::begin(network)) ? std::end(network) : it;
    prev--;

#if 0
    log_info() << prev->second.self() << " " << i << " " << next->second.self();
#endif

    assert(it->second.successor().node.id == next->second.self().node.id);
    assert(it->second.predecessor().node.id == prev->second.self().node.id);

    for (const auto &finger : it->second) {
      auto start = finger.start;
      auto id = finger.node.id;

      /* id ist nächste node nach start */
      assert(successor(start) == id);

      /* kein andere node zwischen start und id */
      assert(start.in(predecessor(id), id));
    }
    i++;
  }
}

int main() {
  const std::string port("8042");
  const std::string seed_node = "10.1";

  network.emplace(hydra::hash(seed_node),
                  hydra::routing_table(seed_node, port));

  for (auto &&node : network)
    std::cout << hydra::hex(node.first) << " " << node.second << std::endl;

  for (size_t i = 2; i < 0xffffff; i++) {
    std::ostringstream s;
#if 0
    s << "10." << i;
#else
    s << i;
#endif
    if (node_exists(hydra::hash(s.str()))) {
      log_info() << "colliding ID with " << s.str() << " and "
                 << hydra::hex(hydra::hash(s.str()));
      continue;
    }

    log_info() << "Adding node " << s.str();

    hydra::routing_table new_table(s.str(), port);
    auto key = new_table.self().node.id;

    init_table(new_table, network[hydra::hash(seed_node)]);

    update_others(new_table);

    check();

    if(network.size() == 255)
      break;
  }

  log_info() << "Checked with " << network.size() << " nodes";
}

