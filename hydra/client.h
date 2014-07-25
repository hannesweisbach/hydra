#pragma once

#include <future>
#include <memory>
#include <functional>
#include <utility>
#include <vector>

#include "hydra/network.h"
#include "hydra/passive.h"

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
  std::unique_ptr<hydra::overlay::network> network;
  passive &responsible_node(const std::vector<unsigned char> &key) const;
};
}

