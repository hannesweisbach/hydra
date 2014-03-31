#include <iostream>
#include <unordered_map>
#include <string>
#include <ostream>

#include "hydra/types.h"
#include "util/Logger.h"

static std::unordered_map<std::string, hydra::routing_table> network;

hydra::routing_entry predecessor(const hydra::routing_table &table,
                                 const hydra::keyspace_t &id) {
  hydra::routing_entry node = table.self();
  hydra::routing_table t = table;
  while (
      !hydra::interval({ static_cast<hydra::keyspace_t>(t.self().node.id + 1),
                         t.successor().node.id }).contains(id)) {
    log_info() << std::hex << (unsigned)id << " !in ("
               << (unsigned)t.self().node.id << ", "
               << (unsigned)t.successor().node.id << "] node: " << t.self();
    auto preceding_node = t.preceding_node(id);
    log_info() << "Preceding node of " << std::hex << (unsigned)id << " is "
               << preceding_node << " " << t;
    t = network[preceding_node.node.ip];
    //hydra::routing_entry pred = predecessor(t, id);
    //node = network[pred.node.ip].self();
    node = t.self();
    log_info() << node << " " << t;
  }
  log_info() << std::hex << (unsigned)id << "  in ("
             << (unsigned)t.self().node.id << ", "
             << (unsigned)t.successor().node.id << ") node: " << t.self();
  return node;
}

hydra::routing_entry successor(const hydra::routing_table &table,
                               const hydra::keyspace_t &id) {
  auto pred = predecessor(table, id);

  log_trace() << std::hex << "Successor of " << (unsigned)id << " is "
              << network[pred.node.ip].successor();
  return network[pred.node.ip].successor();
}

void init_table(hydra::routing_table &n, const hydra::routing_table &n_) {
  const hydra::keyspace_t start = n[0].interval.start;
  log_info() << "I am " << n.self().node;

  n.successor().node = successor(n_, start).node;
  log_info() << "My successor is " << n.successor().node;

  /* read predecessor-pointer from successor */
  n.predecessor().node = network[n.successor().node.ip].predecessor().node;
  log_info() << "My predecessor is " << n.predecessor().node;

  /* write predecessor-pointer to successor */
  log_info() << "My successors predecessor was "
             << network[n.successor().node.ip].predecessor().node;
  network[n.successor().node.ip].predecessor().node = n.self().node;
  log_info() << "My successors predecessor is "
             << network[n.successor().node.ip].predecessor().node;

  std::transform(std::begin(n) + 1, std::end(n), std::begin(n),
                 std::begin(n) + 1,
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
}

void update_table(hydra::routing_table &table, const hydra::routing_entry &s,
                  size_t &i) {
  indent_guard guard(Logger::underlying_stream);
#if 1
  if (hydra::interval({ table.self().node.id,
                        static_cast<hydra::keyspace_t>(table[i].node.id - 1) })
          .contains(s.node.id)) {
    log_info() << indent << std::hex << (unsigned)s.node.id << "  in ["
               << (unsigned)table.self().node.id << ", "
               << (unsigned)table[i].node.id << ") ";
#else
#if 0
  if (table[i].interval.contains(s.node.id)) {
#else
  if (hydra::interval(
          { static_cast<hydra::keyspace_t>(table.self().node.id + 1),
            static_cast<hydra::keyspace_t>(table[i].node.id) })
          .contains(s.node.id)) {
#endif
#endif
    log_info() << indent << "Updating entry " << i << " of " << table.self()
               << " from " << table[i].node << " to " << s.node;
    table[i].node = s.node;
    auto pred = table.predecessor();
    if (pred.node.id != s.node.id) {
      log_info() << indent << "Notifying " << pred << " to update " << s << " "
                 << i;
      update_table(network[pred.node.ip], s, i);
    }
  }
}

void update_others(const hydra::routing_table &table) {
  for (size_t k = 0; k < std::numeric_limits<hydra::keyspace_t>::digits; k++) {
    hydra::keyspace_t key =
        table.self().node.id - static_cast<hydra::keyspace_t>(1 << k);
    log_info() << "Looking for predecessor of " << std::hex << (unsigned)key
               << " in " << table;
    auto pred = predecessor(table, key);
    log_info() << "Pred of " << std::hex << (unsigned)key << " is " << pred;
    if(pred.node.id != table.self().node.id)
      update_table(network[pred.node.ip], table.self(), k);
  }
}

void join(const std::string &node, const std::string &new_node) {
  init_table(network[new_node], network[node]);

  log_info() << network[new_node];

  update_others(network[new_node]);
}

int main() {
  std::string port("8042");
  std::vector<std::string> nodes = { "10.1" };

  for (auto &&node : nodes) {
    network.emplace(node, hydra::routing_table(node, port));
  }

  for (auto &&node : network)
    std::cout << node.first << " " << node.second << std::endl;

  for(size_t i = 2; i < 100; i++) {
    std::ostringstream s;
    s << "10." << i;
    bool skip = false;
    for (auto &&node : network) {
      if (node.second.self().node.id ==
          static_cast<hydra::keyspace_t>(hydra::hash(s.str()))) {
        log_info() << "colliding ID with " << s.str() << " and "
                   << node.second.self().node;
        skip = true;
      }
    }
    if (skip)
      continue;

    hydra::routing_table new_table(s.str(), port);
    log_info() << s.str() << " " << new_table;
    network.emplace(s.str(), new_table);

    join(nodes[0], s.str());
    
    for (auto &&node : network)
      std::cout << node.first << " " << node.second << std::endl;
  }
#if 0
  join(nodes[0], nodes[2]);
  
  for (auto &&node : network)
    std::cout << node.first << " " << node.second << std::endl;
#endif
}
