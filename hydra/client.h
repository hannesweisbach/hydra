#pragma once

#include <future>
#include <memory>
#include <functional>

#include "hydra/passive.h"
#include "hydra/types.h"

#include "rdma/RDMAClientSocket.h"

namespace hydra {
class client {
public:
  client(const std::string &ip, const std::string &port);
  std::future<bool> add(const char *key, const size_t key_length,
                        const char *value, const size_t value_length) const;
  std::future<bool> remove(const char *key, const size_t key_length) const;
  bool contains(const char *key, const size_t key_length) const;
  typedef std::unique_ptr<char, std::function<void(char *)> > value_ptr;
  value_ptr get(const char *key, const size_t key_length) const;

private:
  passive root_node;
  node_id responsible_node(const char *key, const size_t size) const;
  node_info get_info(RDMAClientSocket &socket) const;
};
}

