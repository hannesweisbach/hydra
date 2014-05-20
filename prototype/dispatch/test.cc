#include <iostream>
#include <memory>
#include <thread>
#include <future>
#include <chrono>

#include "dispatch.h"

int main() {
  run_queue r;

  std::promise<void> promise;

  std::cout << "Adding" << std::endl;

  r.add(promise.get_future(), [](std::future<void> f) {
    std::cout << __func__ << " invoked" << std::endl;
    f.get();
    std::cout << "future received" << std::endl;
  });

  std::cout << "Added" << std::endl;

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::cout << "Fulfilling promise" << std::endl;
  promise.set_value();
  std::cout << "Promise fulfilled" << std::endl;

  std::vector<std::promise<int> > ps;
  for (auto &&i : { 1, 2, 3, 4, 5, 6, 7, 8 }) {
    ps.emplace_back();
    std::cout << "Adding i: " << i << std::endl;
    r.add(ps.back().get_future(), [=](std::future<int> f) {
      std::cout << __func__ << " invoked" << std::endl;
      std::cout << "future received: " << f.get() << " " << i << std::endl;
    });
    std::cout << "Promising i: " << i << std::endl;
    ps.back().set_value(i);
  }

  std::cout << "Done." << std::endl;

  std::unique_ptr<int> ptr(new int(1234));
  std::promise<void> unique_promise;
  unique_promise.set_value();
  r.add(unique_promise.get_future(),
        [ptr = std::move(ptr)](std::future<void> f) {
    std::cout << "Unqiue lambda " << *ptr << std::endl;
    f.get();
  });

  std::promise<void> p1;
  std::promise<void> p2;
  r.add(p2.get_future(), [](std::future<void> f) {
    f.get();
    std::cout << "Second" << std::endl;
  });

  r.add(p1.get_future(), [p2 = std::move(p2)](std::future<void> f) mutable {
    f.get();
    std::cout << "First" << std::endl;
    p2.set_value();
  });

  p1.set_value();

  std::promise<void> pr;
  pr.set_value();

  r.add(pr.get_future(), [&](auto f) {
    std::promise<void> pr;
    pr.set_value();
    f.get();
    std::cout << "First level" << std::endl;
    r.add(pr.get_future(), [](std::future<void> f) {
      f.get();
      std::cout << "Second level" << std::endl;
    });
  });

  std::promise<int> pc;

  auto f1 =
      r.add(pc.get_future(),
            [](auto f) { std::cout << "first: " << f.get() << std::endl; });
  auto f2 = r.add(std::move(f1), [](auto f) {
    f.get();
    std::cout << "second" << std::endl;
    return 5;
  });

  auto f3 =
      r.add(std::move(f2),
            [](auto f) { std::cout << "last: " << f.get() << std::endl; });

  std::cout << "Waiting for last" << std::endl;
  pc.set_value(654);
  f3.get();
  std::cout << "got last" << std::endl;

  auto ef = r.add([]() { throw std::runtime_error("foo exception"); });
  try {
    ef.get();
  }
  catch (const std::runtime_error &e) {
    std::cout << "Caught exception " << e.what() << std::endl;
  }
}

