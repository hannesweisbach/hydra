#pragma once

#include <future>
#include <memory>
#include <functional>
#include <utility>

#include "hydra/passive.h"
#include "hydra/types.h"

#include "rdma/RDMAClientSocket.h"

namespace hydra {
class client {
public:
  typedef std::unique_ptr<unsigned char, std::function<void(unsigned char *)> >
  value_ptr;

  client(const std::string &ip, const std::string &port);
  std::future<bool> add(const unsigned char *key, const size_t key_length,
                        const unsigned char *value,
                        const size_t value_length) const;
  std::future<bool> remove(const unsigned char *key,
                           const size_t key_length) const;
  bool contains(const unsigned char *key, const size_t key_length) const;
  value_ptr get(const unsigned char *key, const size_t key_length) const;
  routing_table table() const;

private:
  passive root_node;
  node_id responsible_node(const unsigned char *key, const size_t size) const;
  node_info get_info(const RDMAClientSocket &socket) const;
  std::pair<value_ptr, const size_t>
  find_entry(const RDMAClientSocket &socket, const unsigned char *key,
             const size_t key_length) const;
};
}

