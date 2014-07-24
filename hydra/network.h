#pragma once

#include <utility>
#include <string>
#include <memory>

#include "hydra/passive.h"
#include "hydra/keyspace.h"

namespace hydra {
class network {
public:
  class node {
    std::pair<keyspace_t, keyspace_t> range;
    std::unique_ptr<hydra::passive> node_;

  public:
    node(const keyspace_t &start, const keyspace_t &end,
         const std::string &host, const std::string &port)
        : range(start, end),
          node_(std::make_unique<hydra::passive>(host, port)) {}
    bool contains(const keyspace_t &id) const {
      return id.in(range.first, range.second);
    }
    operator hydra::passive &() { return *node_; }
  };
  virtual ~network() = default;
  
  virtual passive &successor(const keyspace_t &id) = 0;
};
}
