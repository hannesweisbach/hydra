#pragma once

#include <utility>
#include <memory>
#include <future>
#include <cassert>
#include <vector>
#include <list>
#include <mutex>
#include <thread>
#include <algorithm>
#include <iterator>

#include "concurrent.h"

class run_queue {
  class Runnable {
  public:
    template <typename T, typename R>
    Runnable(std::future<T> future, std::packaged_task<R(std::future<T>)> task)
        : self_(new wait_model<T, R>(std::move(future), std::move(task))) {}

    bool ready() const noexcept { return self_->ready(); }
    void operator()() { return (*self_)(); }

  private:
    struct concept {
      virtual ~concept() noexcept = default;
      virtual void operator()() = 0;
      virtual bool ready() const noexcept = 0;
    };

    template <typename T, typename R> struct wait_model final : public concept {
      std::future<T> future;
      std::packaged_task<R(std::future<T>)> task;

      wait_model(std::future<T> future,
                 std::packaged_task<R(std::future<T>)> task)
          : future(std::move(future)), task(std::move(task)) {
        assert(("Future is not valid.", future.valid()));
      }

      bool ready() const noexcept override {
        return future.wait_for(std::chrono::seconds(0)) ==
               std::future_status::ready;
      }
      void operator()() { task(std::move(future)); }
    };

    std::unique_ptr<concept> self_;
  };

  bool running;
  std::thread t;
  std::vector<Runnable*> tasks;
  mutable std::list<Runnable*> new_tasks;
  mutable std::mutex new_task_mutex;

  void merge_new_tasks() {
    std::unique_lock<std::mutex> lock(new_task_mutex);
    for (auto &&runnable : new_tasks) {
      tasks.push_back(std::move(runnable));
    }
    new_tasks.clear();
  }

  void run() {
    while (!tasks.empty() || running) {
      if (running)
        merge_new_tasks();
      auto r = std::find_if(std::begin(tasks), std::end(tasks),
                            std::mem_fn(&Runnable::ready));
      if (r != std::end(tasks)) {
#if 0
        (*r)();
#else

        dispatch_async_f(
            dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), *r,
            [](void *r_) {
              Runnable *r = static_cast<Runnable *>(r_);
              (*r)();
              /* Runnable gets deleted here */
              delete r;
            });
        *r = nullptr;
#endif

        tasks.erase(r);
      } else {
        std::this_thread::yield();
      }
    }
  }

  void emplace_new_task(Runnable *runnable) const {
    std::list<Runnable*> tmp;
    tmp.push_back(std::move(runnable));
    {
      std::unique_lock<std::mutex> lock(new_task_mutex);
      new_tasks.splice(std::end(new_tasks), std::move(tmp));
    }
  }

public:
  run_queue() : running(true), t(&run_queue::run, this) {}

  template <typename T, typename F>
  auto add(std::future<T> future, F &&functor) const {
    using R = typename std::result_of<F(std::future<T>)>::type;
    std::packaged_task<R(std::future<T>)> task{ std::move(functor) };
    auto return_future = task.get_future();
    /* the new'ed up Runnable will be deleted in run_queue::run(). This is not
     * exception safe. */
    emplace_new_task(new Runnable{ std::move(future), std::move(task) });
    return return_future;
  }

  template <typename F> auto add(F &&functor) const {
    return hydra::async(std::forward<F>(functor));
  }

  ~run_queue() {
    add([=]() { running = false; });
    if (t.joinable())
      t.join();
    if (tasks.size())
      std::cout << tasks.size() << " tasks remaining." << std::endl;
    for (auto &&r : tasks)
      delete r;
  }
};

extern run_queue r__;

namespace hydra {
namespace dispatch {
template <typename T, typename F>
inline auto then(std::future<T> future, F &&functor) {
  return r__.add(std::move(future), std::forward<F>(functor));
}
}
}

