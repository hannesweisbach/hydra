#pragma once

#include <type_traits>
#include <vector>

namespace hydra {
namespace rdma {

namespace _ {
template <typename T> const void *address_of(const T &o, std::true_type) {
  return o;
}
template <typename T> const void *address_of(const T &o, std::false_type) {
  return &o;
}

template <typename T> size_t size_of(const T &, std::true_type) {
  return sizeof(std::remove_pointer<T>::type);
}
template <typename T> size_t size_of(const T &, std::false_type) {
  return sizeof(T);
}
}

template <typename T> const void *address_of(const T &o) {
  return _::address_of(o, std::is_pointer<T>());
}

template <typename T> const void *address_of(const std::vector<T> &o) {
  return o.data();
}

template <typename T> size_t size_of(const T &o) {
  return _::size_of<T>(o, std::is_pointer<T>());
}

template <typename T> size_t size_of(const std::vector<T> &o) {
  return o.size() * sizeof(T);
}
}
}
