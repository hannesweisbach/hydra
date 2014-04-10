#pragma once

#include "hash.h"
#include "rdma/RDMAWrapper.hpp"

template <typename T> class RDMAObj {
protected:
  T obj;
  uint64_t crc;

public:
  template <typename... Args>
  RDMAObj(Args &&... args)
      : obj(std::forward<Args>(args)...), crc(hydra::hash64(&obj)) {}

  void rehash() { crc = hydra::hash64(&obj); }
  bool valid() const { return hydra::hash64(&obj) == crc; }
  const T &get() const { return obj; }
};

template <typename T> class LocalRDMAObj : public RDMAObj<T> {
  
public:
  template <typename... Args>
  LocalRDMAObj(Args &&... args)
      : RDMAObj<T>(std::forward<Args>(args)...) {}
#if 0
  template <typename F>
  auto operator()(F &&f) const -> typename std::result_of<F(const T &)>::type {
    return f(obj);
  }
#endif
  
  template <typename F, typename = typename std::enable_if<
                            !std::is_same<typename std::result_of<F(T &)>::type,
                                          void>::value>::type>
  auto operator()(F &&f) -> typename std::result_of<F(T &)>::type {
    auto ret = f(RDMAObj<T>::obj);
    RDMAObj<T>::rehash();
    //RDMAObj<T>::crc = hydra::hash64(&RDMAObj<T>::obj);
    return ret;
  }

  template <typename F,
            typename = typename std::enable_if<std::is_same<
                typename std::result_of<F(T &)>::type, void>::value>::type>
  void operator()(F &&f) {
    f(RDMAObj<T>::obj);
    //RDMAObj<T>::crc = hydra::hash64(&LocalRDMAObj<T>::obj);
    RDMAObj<T>::rehash();
  }
};

