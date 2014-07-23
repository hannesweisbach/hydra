#pragma once

#include "hash.h"
#include "rdma/RDMAWrapper.hpp"
#include <stdint.h>

template <typename T> class RDMAObj {
protected:
  T obj;
  uint64_t crc;

public:
  template <typename... Args>
  RDMAObj(Args &&... args)
      : obj(std::forward<Args>(args)...), crc(hydra::hash64(&obj)) {}
  RDMAObj(const RDMAObj<T> &other) = default;
  RDMAObj<T> &operator=(const RDMAObj<T> &other) = default;
  RDMAObj(RDMAObj<T> &&other)
      : obj(std::move(other.obj)), crc(hydra::hash64(&obj)) {
    other.rehash();
  }
  RDMAObj<T> &operator=(RDMAObj<T> &&other) {
    obj = std::move(other.obj);
    other.rehash();
    rehash();
    return *this;
  }

  void rehash() { crc = hydra::hash64(&obj); }
  bool valid() const { return hydra::hash64(&obj) == crc; }
  const T &get() const { return obj; }
};

template <typename T> class LocalRDMAObj : public RDMAObj<T> {

  template <typename F> void void_helper(F &&f, std::true_type) {
    f(RDMAObj<T>::obj);
    RDMAObj<T>::rehash();
  }

  template <typename F> auto void_helper(F &&f, std::false_type) {
    auto ret = f(RDMAObj<T>::obj);
    RDMAObj<T>::rehash();
    return ret;
  }

public:
  using RDMAObj<T>::RDMAObj;
  using RDMAObj<T>::operator=;

  template <typename F> auto operator()(F &&f) {
    return void_helper(std::forward<F>(f),
                       std::is_void<typename std::result_of<F(T &)>::type>());
  }
};

namespace hydra {
namespace rdma {
template <typename Socket, typename T>
void load(const Socket &s, RDMAObj<T> &o, const ibv_mr *mr, uintptr_t remote,
          uint32_t rkey, size_t retries = 1) {
  do {
    s.read(o, mr, remote, rkey);
    retries--;
  } while (!o.valid() && retries);

  if (!o.valid())
    throw std::runtime_error("Could not validate remote object");
}
}
}
