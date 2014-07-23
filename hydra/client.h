#pragma once

#include <future>
#include <memory>
#include <functional>
#include <utility>
#include <vector>

#include "hydra/chord.h"
#include "hydra/types.h"

#include "rdma/RDMAClientSocket.h"


namespace hydra {
class client {
public:
  client(const std::string &ip, const std::string &port);
  bool add(const std::vector<unsigned char> &key,
           const std::vector<unsigned char> &value) const;
  bool remove(const std::vector<unsigned char> &key) const;
  bool contains(const std::vector<unsigned char> &key) const;
  std::vector<unsigned char> get(const std::vector<unsigned char> &key) const;

private:
  chord::node root;
  node_id responsible_node(const std::vector<unsigned char> &key) const;
};
}

