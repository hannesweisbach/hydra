#include "future.h"

namespace hydra {
namespace detail {

void shared_state<void>::set_exception(std::exception_ptr p) {
  set_(boost::make_unexpected(std::move(p)));
}

void shared_state<void>::set() {
  set_(expected_type(boost::in_place_t{}));
}

void shared_state<void>::set_(expected_type value) {
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

shared_state<void>::expected_type shared_state<void>::get() {
  wait();
  return data;
}

void shared_state<void>::wait() {
  for (;;) {
    {
      std::unique_lock<decltype(state_lock)> l(state_lock);
      if (satisfied)
        break;
    }
    std::this_thread::yield();
  }
}
}

promise<void>::promise()
    : state(std::make_shared<detail::shared_state<void> >()) {}
promise<void>::promise(promise &&) = default;

future<void> promise<void>::get_future() {
  check_state();
  if (have_future) {
    throw std::future_error(
        std::make_error_code(std::future_errc::future_already_retrieved));
  }

  have_future = true;
  return future<void>(state);
}

void promise<void>::set_value() {
  check_state();
  state->set();
}

void promise<void>::set_exception(std::exception_ptr p) {
  check_state();
  state->set_exception(std::move(p));
}

void promise<void>::check_state() {
  if (!state) {
    throw std::future_error(std::make_error_code(std::future_errc::no_state));
  }
}

future<void>::future() = default;
future<void>::future(future &&) = default;

future<void>::future(std::shared_ptr<detail::shared_state<void> > state_)
    : state(state_) {}

typename future<void>::expected_type future<void>::get() {
  check_state();
  auto tmp(state->get());
  state.reset();
  return tmp;
}

bool future<void>::valid() const {
  return static_cast<bool>(state);
}

void future<void>::wait() const {
  check_state();
  state->wait();
}

void future<void>::check_state() const {
  if (!state) {
    throw std::future_error(std::make_error_code(std::future_errc::no_state));
  }
}
}
