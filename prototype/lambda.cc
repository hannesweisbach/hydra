#include <string>
#include <thread>
#include <future>
#include <iostream>

template <typename Functor>
auto execute(Functor &&functor)
    -> std::future<typename std::result_of<Functor()>::type> {
  using result_type = typename std::result_of<Functor()>::type;
  using packaged_type = std::packaged_task<result_type()>;
  auto p = std::make_shared<packaged_type>(std::forward<Functor>(functor));

  std::thread([=]() { (*p)(); }).detach();
  return p->get_future();
}

int main() {
  execute([]() { std::cout << "Hello World" << std::endl; }).get();
}
