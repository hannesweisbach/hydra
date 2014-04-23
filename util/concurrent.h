#pragma once
#include <memory>
#include <future>
#include <mutex>
#include <list>
#include <string>
#include <iostream>
#include <utility>
#include <atomic>

#include <assert.h>

#include "util/Logger.h"

template <typename T> class concurrent_queue {
  mutable std::list<T> q;
  mutable std::mutex m;
  mutable std::condition_variable not_empty;

public:
  concurrent_queue() {}

  void push(T t) {
    std::list<T> tmp;
    tmp.push_back(t);

    {
      std::unique_lock<std::mutex> _{ m };
      q.splice(q.end(), tmp);
    }

    not_empty.notify_one();
  }

  T pop() {
    std::unique_lock<std::mutex> lock{ m };
    while (q.size() == 0) {
      not_empty.wait(lock);
    }
    T item = q.front();
    q.pop_front();
    return item;
  }
};

template <typename T> struct adv {
  T t;
  explicit adv(T &&t) : t(std::forward<T>(t)) {}
  template <typename... U> T &&operator()(U &&...) {
    return std::forward<T>(t);
  }
};

template <typename T> adv<T> make_adv(T &&t) {
  return adv<T>{ std::forward<T>(t) };
}

namespace std {
template <typename T> struct is_bind_expression<adv<T> > : std::true_type {};
}

template <typename Future, typename Functor, typename Result> struct helper {
  Future future;
  Functor functor;

  helper(Future future, Functor functor)
      : future(std::move(future)), functor(std::move(functor)) {}
  helper(const helper &other) : future(other.future), functor(other.functor) {}
  helper(helper &&other)
      : future(std::move(other.future)), functor(std::move(other.functor)) {}
  helper &operator=(helper other) {
    future = std::move(other.future);
    functor = std::move(other.functor);
    return *this;
  }

  Result operator()() {
    future.wait();
    return functor(std::move(future));
  }
};

namespace hydra {
class spinlock {
  std::string name;
  std::atomic<uint64_t> locks;
  std::atomic<uint64_t> spins;
  std::atomic<uint64_t> yields;
  std::atomic_flag lock_;

public:
  spinlock() noexcept;
  spinlock(const std::string&) noexcept;
  void lock() noexcept;
  void unlock() noexcept;
  void __debug() noexcept;
};
}

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
typedef dispatch_queue_t async_queue_type;

namespace hydra {

template <typename F, typename... Args>
auto async(dispatch_queue_t q, F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using result_type = typename std::result_of<F(Args...)>::type;
  using packaged_type = std::packaged_task<result_type()>;

  // auto p = new packaged_type(std::bind<result_type>(std::forward<F>(f),
  // std::forward<Args>(args)...));
  auto p = new packaged_type(std::bind<result_type>(
      std::forward<F>(f), make_adv(std::forward<Args>(args))...));
  auto result = p->get_future();

  dispatch_async_f(q, p, [](void *f_) {
    packaged_type *f = static_cast<packaged_type *>(f_);
    (*f)();
    delete f;
  });

  return result;
}

/* whuä. F might be instantiated with dispatch_queue_s *&
 * I don't know how to avoid this properly - if you know how, please let me
 * know.
 */
template <typename F, typename... Args,
          typename = typename std::enable_if<
              !std::is_same<F, dispatch_queue_t>::value>::type>
auto async(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  return hydra::async(
      dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
      std::forward<F>(f), std::forward<Args>(args)...);
}

template <typename Future, typename Functor>
auto then(Future &&future, Functor &&functor)
    -> std::future<typename std::result_of<Functor(Future)>::type> {
  return hydra::async(
      helper<Future, Functor, typename std::result_of<Functor(Future)>::type>(
          std::move(future), std::move(functor)));
}
}

#if 0
template <typename T> class monitor {
  std::string name;
  mutable dispatch_queue_t queue;
  mutable T data_;

public:
  monitor() : monitor(T()) {}
  template <typename... Args>
  monitor(Args &&... args)
      : name("hydra.queue.monitor" +
             std::to_string(reinterpret_cast<uintptr_t>(this))),
        queue(dispatch_queue_create(name.c_str(), NULL)),
        data_(std::forward<Args>(args)...) {
    assert(queue);
  }
  ~monitor() { dispatch_release(queue); }

  monitor(monitor<T> &&other)
      : name(std::move(other.name)), queue(std::move(other.queue)),
        data_(std::move(other.data_)) {}

  monitor<T>& operator=(monitor<T>&& other) {
    std::swap(name, other.name);
    std::swap(queue, other.queue);
    std::swap(data_, other.data_);

    return *this;
  }

  template <typename F>
  auto operator()(F &&f)
      const -> std::future<typename std::result_of<F(T &)>::type> {
    return hydra::async(queue, std::forward<F>(f), data_);
  }
};
#else
template <typename T> class monitor {
  mutable T data_;
  mutable hydra::spinlock mutex_;

public:
  monitor() = default;
  template <typename... Args>
  monitor(const std::string &name, Args &&... args)
      : data_(std::forward<Args>(args)...), mutex_(name) {}

#if 0
  monitor<T>& operator=(monitor<T>&& other) {
    std::swap(data_, other.data_);
    std::swap(mutex_, other.mutex_);

    return *this;
  }
#endif

  template <typename F, typename = typename std::enable_if<
                            !std::is_same<typename std::result_of<F(T &)>::type,
                                          void>::value>::type>
  auto operator()(F &&f)
      const -> std::future<typename std::result_of<F(T &)>::type> {
#if 0
    return std::async(std::launch::async, [
                                            &,
                                            f_ = std::forward<F>(f)
                                          ]() {
      std::unique_lock<std::mutex> lock(mutex_);
      return f_(data_);
    });
#endif
    std::promise<typename std::result_of<F(T &)>::type> promise;
    {
      std::unique_lock<hydra::spinlock> lock(mutex_);
      promise.set_value(f(data_));
    }
    //mutex_.__debug();
    return promise.get_future();
  }

  template <typename F,
            typename = typename std::enable_if<std::is_same<
                typename std::result_of<F(T &)>::type, void>::value>::type>
  auto operator()(F &&f) const -> std::future<void> {
    std::promise<void> promise;
    {
      std::unique_lock<hydra::spinlock> lock(mutex_);
      f(data_);
    }
    promise.set_value();
    //mutex_.__debug();
    return promise.get_future();
  }
};

#endif

#else /* HAVE_LIBDISPATCH */

#include <mutex>
typedef int async_queue_type;

template <typename T> class monitor {
  mutable T data_;
  mutable std::mutex mutex_;

public:
  monitor() = default;
  template <typename... Args>
  monitor(Args &&... args)
      : data_(std::forward<Args>(args)...) {}

  monitor<T>& operator=(monitor<T>&& other) {
    std::swap(data_, other.data_);
    std::swap(mutex_, other.mutex_);

    return *this;
  }

  template <typename F>
  auto operator()(F &&f)
      const -> std::future<typename std::result_of<F(T &)>::type> {
    return std::async(std::launch::async, [
                                            &,
                                            f_ = std::forward<F>(f)
                                          ]() {
      std::unique_lock<std::mutex> lock(mutex_);
      return f_(data_);
    });
  }
};

namespace hydra {

template <typename F, typename... Args>
auto async(async_queue_type q, F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  (void)(q);
  return std::async(std::launch::async, std::forward<F>(f),
                    std::forward<Args>(args)...);
}

template <typename F, typename... Args,
          typename = typename std::enable_if<
              !std::is_same<F, async_queue_type>::value>::type>
auto async(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  return std::async(std::launch::async, std::forward<F>(f),
                    std::forward<Args>(args)...);
}

template <typename Future, typename Functor>
auto then(Future &&future, Functor &&functor)
    -> std::future<typename std::result_of<Functor(Future)>::type> {
  return std::async(
      std::launch::async,
      helper<Future, Functor, typename std::result_of<Functor(Future)>::type>(
          std::move(future), std::move(functor)));
}
}

#endif /* HAVE_LIBDISPATCH */

