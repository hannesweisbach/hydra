#pragma once

#include <future>
#include <memory>
#include <functional>
#include <utility>
#include <vector>

#include "hydra/passive.h"
#include "hydra/types.h"

#include "rdma/RDMAClientSocket.h"


namespace hydra {

hydra::node_info get_info(const RDMAClientSocket &socket);

class client {
public:
  typedef std::unique_ptr<unsigned char, std::function<void(unsigned char *)> >
  value_ptr;

  client(const std::string &ip, const std::string &port);
  bool add(const std::vector<unsigned char> &key,
           const std::vector<unsigned char> &value) const;
  bool remove(const std::vector<unsigned char> &key) const;
  bool contains(const std::vector<unsigned char> &key) const;
  std::vector<unsigned char> get(const std::vector<unsigned char> &key) const;
  routing_table table() const;

private:
  passive root_node;
  node_id responsible_node(const std::vector<unsigned char> &key) const;
  node_info get_info(const RDMAClientSocket &socket) const;
  std::pair<value_ptr, const size_t>
  find_entry(const RDMAClientSocket &socket,
             const std::vector<unsigned char> &key) const;
};
}

