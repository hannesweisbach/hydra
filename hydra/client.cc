#include "hydra/client.h"
#include "hydra/chord.h"
#include "hydra/hash.h"

#include "util/Logger.h"

hydra::client::client(const std::string &ip, const std::string &port)
    : root_node(ip, port) {}

hydra::node_id hydra::client::responsible_node(const char *key,
                                               const size_t length) const {
  return chord::successor(
      root_node.table(),
      static_cast<hydra::keyspace_t::value_type>(hydra::hash(key, length)));
}

std::future<bool> hydra::client::add(const char *key, const size_t key_length,
                                     const char *value,
                                     const size_t value_length) const {
  auto nodeid = responsible_node(key, key_length);
  /* TODO: expose map/malloc interface in hydra::passive */
  /* TODO: move passive::add code here */
  hydra::passive node(nodeid.ip, nodeid.port);
  return node.add(key, key_length, value, value_length);
}

std::future<bool> hydra::client::remove(const char *key,
                                        const size_t key_length) const {
  auto nodeid = responsible_node(key, key_length);
  hydra::passive node(nodeid.ip, nodeid.port);
  return node.remove(key, key_length);
}

bool hydra::client::contains(const char *key, const size_t key_length) const {
  auto nodeid = responsible_node(key, key_length);
  hydra::passive node(nodeid.ip, nodeid.port);
  return node.contains(key, key_length);
}

/* alloc: return managed array or std::unique_ptr<char[]>
 * or { char[], size, unqiue_ptr<ibv_mr> }
 * this should also play nice with read.
 */
hydra::client::value_ptr hydra::client::get(const char *key,
                                            const size_t key_length) const {
  auto nodeid = responsible_node(key, key_length);
  hydra::passive node(nodeid.ip, nodeid.port);
  return node.get(key, key_length);
}

