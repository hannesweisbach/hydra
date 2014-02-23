#pragma once

#include <list>
#include <mutex>
#include <thread>
#include <future>
#include <condition_variable>
#include <functional>

#include "util/Logger.h"

class WorkerThread {
  std::condition_variable empty;
  std::mutex list_lock;
  std::list<std::function<void()> > queue;
  std::thread thread;

  bool done = false;

  void process_work();

public:
  WorkerThread();
  ~WorkerThread();
  template <typename Func, typename... Args>
  auto send(Func &&work, Args &&... args)
      -> std::future<typename std::result_of<Func(Args...)>::type> {
    using result_type = typename std::result_of<Func(Args...)>::type;
    using packaged_type = std::packaged_task<result_type()>;

    auto p = new packaged_type(std::bind<result_type>(
        std::forward<Func>(work), std::forward<Args>(args)...));
    auto future = p->get_future();

    std::list<std::function<void()> > tmp;
#if 0
    tmp.push_back(
        std::bind(std::forward<Func>(work), std::forward<Args>(args)...));
#else
    tmp.push_back([=]() {
      (*p)();
      delete p;
    });
#endif
    {
      std::lock_guard<std::mutex> lock(list_lock);
      queue.splice(queue.end(), tmp);
    }
    empty.notify_one();
    return future;
  }
};

