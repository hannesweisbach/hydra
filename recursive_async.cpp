#include <iostream>
#include <future>
#include <memory>
#include "util/concurrent.h"

struct s {
  int a;
};

template <typename T> std::future<T> post(T item) {
  auto promise = std::make_shared<std::promise<T> >();
  std::function<void()> *f = new std::function<void()>();
  *f = [=]() {
    try {
      promise->set_value(item);
    }
    catch (std::exception &) {
      promise->set_exception(std::current_exception());
    }
    delete f;
  };

  hydra::async(*f);

  return promise->get_future();
}

void func1(std::shared_ptr<s> p) {
  std::future<std::shared_ptr<s> > f = post(p);
  hydra::then(std::move(f), [](std::future<std::shared_ptr<s>> p_) {
    auto p__ = p_.get();
    std::cout << p__->a++ << std::endl;
    hydra::async(std::bind(func1, p__));
  });
}

int main() {
  auto i = std::make_shared<s>();
  func1(i);
  func1(std::make_shared<s>());
  dispatch_main();
}
