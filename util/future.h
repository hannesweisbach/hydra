#pragma once

#include <memory>
#include <future>
#include <atomic>
#include <exception>
#include <thread>
#include <mutex>

#include <iostream>

#include <boost/expected/expected.hpp>
#include <dispatch/dispatch.h>
#include "util/concurrent.h"

namespace hydra {

template <typename T> class future;
template <typename T> class promise;

namespace detail {
template <typename T> class shared_state;
}

template <typename T> class promise {
public:
  promise() : state(std::make_shared<detail::shared_state<T> >()) {}
  promise(promise &&) = default;
  promise(const promise &) = delete;

  future<T> get_future() {
    check_state();
    if (have_future) {
      throw std::future_error(
          std::make_error_code(std::future_errc::future_already_retrieved));
    }

    have_future = true;
    return future<T>(state);
  }

  void set_value(const T &value) {
    check_state();
    state->set(value);
  }

  void set_value(T &&value) {
    check_state();
    state->set(std::move(value));
  }

  void set_exception(std::exception_ptr p) {
    check_state();
    state->set_exception(std::move(p));
  }

private:
  void check_state() {
    if (!state) {
      throw std::future_error(std::make_error_code(std::future_errc::no_state));
    }
  }
  bool have_future = false;
  std::shared_ptr<detail::shared_state<T> > state;
};

template <> class promise<void> {
public:
  promise();
  promise(promise &&);
  promise(const promise &) = delete;

  future<void> get_future();
  void set_value();
  void set_exception(std::exception_ptr p);

private:
  void check_state();
  bool have_future = false;
  std::shared_ptr<detail::shared_state<void> > state;
};

template <typename T> class future {
  using expected_type = boost::expected<T, std::exception_ptr>;

public:
  future();
  future(future &&other);
  future(const future &) = delete;
  future &operator=(future &&) = default;
  future &operator=(const future &) = delete;

  void swap(future &rhs) {
    using std::swap;
    swap(state, rhs.state);
  }

  expected_type get();
  bool valid() const;
  void wait() const;

  template <typename Functor>
  auto then(Functor &&f) -> future<decltype(f(std::declval<expected_type>()))>;

private:
  void check_state() const;
  friend class promise<T>;
  future(std::shared_ptr<detail::shared_state<T> > state_);
  std::shared_ptr<detail::shared_state<T> > state;
};

template <> class future<void> {
  using expected_type = boost::expected<void, std::exception_ptr>;

public:
  future();
  future(future &&other);
  future(const future &) = delete;
  future &operator=(future &&) = default;
  future &operator=(const future &) = delete;

  void swap(future &rhs) {
    using std::swap;
    swap(state, rhs.state);
  }

  expected_type get();
  bool valid() const;
  void wait() const;

  template <typename Functor>
  auto then(Functor &&f) -> future<decltype(f(std::declval<expected_type>()))>;

private:
  void check_state() const;
  friend class promise<void>;
  future(std::shared_ptr<detail::shared_state<void> > state_);
  std::shared_ptr<detail::shared_state<void> > state;
};

template <typename T> class future<future<T> > {
  using expected_type = boost::expected<future<T>, std::exception_ptr>;

public:
  future();
  future(future &&other);
  future(const future &) = delete;
  future &operator=(future &&) = default;
  future &operator=(const future &) = delete;

  void swap(future &rhs) {
    using std::swap;
    swap(state, rhs.state);
  }

  expected_type get() {
    check_state();
    auto tmp(state->get());
    state.reset();
    return tmp;
  }

  bool valid() const { return static_cast<bool>(state); }
  void wait() const {
    check_state();
    state->wait();
  }

  template <typename Functor>
  auto then(Functor &&f) -> future<decltype(f(std::declval<expected_type>()))> {
    check_state();
    auto tmp = state->set_continuation(std::forward<Functor>(f));
    state.reset();
    return tmp;
  }

  template <typename = std::enable_if<std::is_same<T, void>::value> >
  future<T> unwrap() {
    promise<T> promise;
    auto future = promise.get_future();

    then([promise = std::move(promise)](auto && inner_future) mutable {
      try {
        inner_future.value()
            .then([promise = std::move(promise)](auto && value) mutable {
               try {
                 value.value();
                 promise.set_value();
               }
               catch (...) {
                 promise.set_exception(std::current_exception());
               }
             });
      }
      catch (...) {
        promise.set_exception(std::current_exception());
      }
    });

    return future;
  }

private:
  void check_state() const {
    if (!state) {
      throw std::future_error(std::make_error_code(std::future_errc::no_state));
    }
  }
  friend class promise<future<T> >;
  future(std::shared_ptr<detail::shared_state<future<T> > > state_)
      : state(state_) {}
  std::shared_ptr<detail::shared_state<future<T> > > state;
};

namespace detail {

#if 0
using lock_type = std::mutex;
#else
using lock_type = hydra::spinlock;
#endif


template <typename Callable> void schedule_task(Callable &&c) {
  using packaged_type = std::packaged_task<void(void)>;
  auto queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
  auto task = new packaged_type(std::forward<Callable>(c));
  dispatch_async_f(queue, task, [](void *erased_ptr) {
    packaged_type *packaged_task = static_cast<packaged_type *>(erased_ptr);
    (*packaged_task)();
    delete packaged_task;
  });
}

template <typename T> class continuation {
public:
  template <typename Callable, typename R>
  continuation(Callable &&c, promise<R> p)
      : impl(std::make_unique<model<Callable, R> >(std::forward<Callable>(c),
                                                   std::move(p))) {}

  void dispatch(boost::expected<T, std::exception_ptr> value) {
    impl->dispatch(std::move(value));
  }

private:
  class concept {
  public:
    virtual ~concept() = default;
    virtual void dispatch(boost::expected<T, std::exception_ptr>) = 0;
  };

  template <typename Callable, typename R> class model final : public concept {
  public:
    model(Callable &&c, promise<R> p)
        : c_(std::forward<Callable>(c)), promise(std::move(p)) {}
    virtual ~model() = default;

    virtual void
    dispatch(boost::expected<T, std::exception_ptr> value) override {
      schedule_task([
        c = std::move(c_),
        promise = std::move(promise),
        value = std::move(value)
      ]() mutable {
          try{
            promise.set_value(c(std::move(value)));
          } catch (...) {
            try {
              promise.set_exception(std::current_exception());
            }
            catch (...) {
            }
          }
        });
    }

  private:
    Callable c_;
    class promise<R> promise;
  };

  template <typename Callable>
  class model<Callable, void> final : public concept {
  public:
    model(Callable &&c, promise<void> p)
        : c_(std::forward<Callable>(c)), promise(std::move(p)) {}
    virtual ~model() = default;

    virtual void
    dispatch(boost::expected<T, std::exception_ptr> value) override {
      schedule_task([
        c = std::move(c_),
        promise = std::move(promise),
        value = std::move(value)
      ]() mutable { 
          try{
            c(std::move(value));
            promise.set_value();
          } catch (...) {
            try {
              promise.set_exception(std::current_exception());
            }
            catch (...) {
            }
          }
       });
    }

  private:
    Callable c_;
    class promise<void> promise;
  };

  std::unique_ptr<concept> impl;
};

template <typename T>
class shared_state /*: public std::enable_shared_from_this<shared_state<T> >*/ {
  using expected_type = boost::expected<T, std::exception_ptr>;

public:
  shared_state() : satisfied(false) {}
  shared_state(shared_state &&) = delete;
  shared_state(const shared_state &) = delete;

  void set(const T &value) { set_(value); }
  void set(T &&value) { set_(std::move(value)); }
  void set_exception(std::exception_ptr p) {
    set_(boost::make_unexpected(std::move(p)));
  }

  expected_type get() {
    wait();
    return data;
  }

  void wait() {
    for (;;) {
      {
        std::unique_lock<decltype(state_lock)> l(state_lock);
        if (satisfied)
          break;
      }
      std::this_thread::yield();
    }
  }

  template <typename Functor,
            typename R = typename std::result_of<Functor(expected_type)>::type>
  auto set_continuation(Functor &&f) -> future<R> {
    std::unique_lock<decltype(state_lock)> l(state_lock);

    promise<R> promise;
    auto future = promise.get_future();

    continuation_ = std::make_unique<continuation<T> >(std::forward<Functor>(f),
                                                       std::move(promise));
    if (satisfied) {
      continuation_->dispatch(std::move(data));
    }

    return future;
  }

private:
  void set_(expected_type value) {
    std::unique_lock<decltype(state_lock)> l(state_lock);
    if (satisfied) {
      throw std::future_error(
          std::make_error_code(std::future_errc::promise_already_satisfied));
    } else if (continuation_) {
      continuation_->dispatch(expected_type(std::move(value)));
    } else {
      data = std::move(value);
    }
    satisfied = true;
  }

  expected_type data;
  std::atomic_bool satisfied;
  typename detail::lock_type state_lock;
  std::unique_ptr<continuation<T> > continuation_;
};

template <> class shared_state<void> {
  using expected_type = boost::expected<void, std::exception_ptr>;

public:
  shared_state() : satisfied(false) {}
  shared_state(shared_state &&) = delete;
  shared_state(const shared_state &) = delete;

  void set();
  void set_exception(std::exception_ptr p);
  expected_type get();
  void wait();

  template <typename Functor,
            typename R = typename std::result_of<Functor(expected_type)>::type>
  auto set_continuation(Functor &&f)
      -> future<decltype(f(std::declval<expected_type>()))> {
    std::unique_lock<decltype(state_lock)> l(state_lock);

    promise<R> promise;
    auto future = promise.get_future();

    continuation_ = std::make_unique<continuation<void> >(
        std::forward<Functor>(f), std::move(promise));
    if (satisfied) {
      continuation_->dispatch(std::move(data));
    }

    return future;
  }

private:
  void set_(expected_type value);
  expected_type data;
  std::atomic_bool satisfied;
  typename detail::lock_type state_lock;
  std::unique_ptr<continuation<void> > continuation_;
};
}

template <typename T> future<T>::future() = default;
template <typename T> future<T>::future(future &&) = default;
template <typename T>
future<T>::future(std::shared_ptr<detail::shared_state<T> > state_)
    : state(state_) {}

template <typename T> typename future<T>::expected_type future<T>::get() {
  check_state();
  auto tmp(state->get());
  state.reset();
  return tmp;
}

template <typename T> bool future<T>::valid() const {
  return static_cast<bool>(state);
}

template <typename T> void future<T>::wait() const {
  check_state();
  state->wait();
}

template <typename T>
template <typename Functor>
auto future<T>::then(Functor &&f)
    -> future<decltype(f(std::declval<future<T>::expected_type>()))> {
  check_state();
  auto tmp = state->set_continuation(std::forward<Functor>(f));
  state.reset();
  return tmp;
}

template <typename T> void future<T>::check_state() const {
  if (!state) {
    throw std::future_error(std::make_error_code(std::future_errc::no_state));
  }
}

template <typename Functor>
auto future<void>::then(Functor &&f)
    -> future<decltype(f(std::declval<future<void>::expected_type>()))> {
  check_state();
  auto tmp = state->set_continuation(std::forward<Functor>(f));
  state.reset();
  return tmp;
}

template <typename T> void swap(future<T> &lhs, future<T> &rhs) {
  lhs.swap(rhs);
}
void inline swap(future<void> &lhs, future<void> &rhs) { lhs.swap(rhs); }
}

