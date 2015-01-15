#include <iostream>
#include <iomanip>
#include <future>
#include <thread>
#include <chrono>
#include <atomic>

#include "gtest/gtest.h"

#include "util/future.h"

using namespace hydra;

TEST(Future, Invalid) {
  future<int> f;
  EXPECT_FALSE(f.valid());
}

TEST(Future, Valid) {
  promise<int> p;
  EXPECT_TRUE(p.get_future().valid());
}

TEST(Future, SetValue) {
  promise<int> p;
  auto future = p.get_future();
  p.set_value(42);

  EXPECT_EQ(42, future.get().value());
}

TEST(Future, DoubleGet) {
  promise<int> p;
  auto future = p.get_future();

  p.set_value(42);

  future.get();
  EXPECT_THROW(future.get(), std::future_error);
}

TEST(Future, DoubleGetFuture) {
  promise<int> p;
  p.get_future();
  EXPECT_THROW(p.get_future(), std::future_error);
}

TEST(Future, DoubleSet) {
  promise<int> p;
  p.set_value(42);
  EXPECT_THROW(p.set_value(43), std::future_error);
}

TEST(Future, SetExceptionGetValue) {
  promise<int> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  auto future = p.get_future();
  EXPECT_THROW(future.get().value(), std::runtime_error);
}

TEST(Future, SetExceptionGetError) {
  promise<int> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  auto future = p.get_future();
  EXPECT_THROW(std::rethrow_exception(future.get().error()),
               std::runtime_error);
}

TEST(Future, DoubleSetException) {
  promise<int> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
    EXPECT_THROW(p.set_exception(std::current_exception()), std::future_error);
  }
}

TEST(Future, DobleSetValueException) {
  promise<int> p;
  p.set_value(32);
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    EXPECT_THROW(p.set_exception(std::current_exception()), std::future_error);
  }
}

TEST(Future, DobleSetExceptionValue) {
  promise<int> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  EXPECT_THROW(p.set_value(32), std::future_error);
}

TEST(Future, AsyncValue) {
  promise<int> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(10ms);
    p.set_value(41);
  });

  t.detach();
  EXPECT_EQ(future.get().value(), 41);
}

TEST(Future, AsyncException) {
  promise<int> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(110ms);
    try {
      throw std::runtime_error("Test");
    }
    catch (const std::runtime_error &) {
      p.set_exception(std::current_exception());
    }
  });

  t.detach();
  EXPECT_THROW(future.get().value(), std::runtime_error);
}

TEST(Future, InvalidVoid) {
  future<void> f;
  EXPECT_FALSE(f.valid());
}

TEST(Future, ValidVoid) {
  promise<void> p;
  EXPECT_TRUE(p.get_future().valid());
}

TEST(Future, SetVoid) {
  promise<void> p;
  auto future = p.get_future();
  p.set_value();

  EXPECT_NO_THROW(future.get().value());
}

TEST(Future, DoubleGetVoid) {
  promise<void> p;
  auto future = p.get_future();

  p.set_value();

  future.get();
  EXPECT_THROW(future.get(), std::future_error);
}

TEST(Future, DoubleGetFutureVoid) {
  promise<void> p;
  p.get_future();
  EXPECT_THROW(p.get_future(), std::future_error);
}

TEST(Future, DoubleSetVoid) {
  promise<void> p;
  p.set_value();
  EXPECT_THROW(p.set_value(), std::future_error);
}

TEST(Future, SetExceptionGetVoid) {
  promise<void> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  auto future = p.get_future();
  EXPECT_THROW(future.get().value(), std::runtime_error);
}

TEST(Future, SetExceptionGetErrorVoid) {
  promise<void> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  auto future = p.get_future();
  EXPECT_THROW(std::rethrow_exception(future.get().error()),
               std::runtime_error);
}

TEST(Future, DoubleSetExceptionVoid) {
  promise<void> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
    EXPECT_THROW(p.set_exception(std::current_exception()), std::future_error);
  }
}

TEST(Future, DobleSetVoidException) {
  promise<void> p;
  p.set_value();
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    EXPECT_THROW(p.set_exception(std::current_exception()), std::future_error);
  }
}

TEST(Future, DobleSetExceptionVoid) {
  promise<void> p;
  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  EXPECT_THROW(p.set_value(), std::future_error);
}

TEST(Future, AsyncVoid) {
  promise<void> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(42ms);
    p.set_value();
  });

  t.detach();
  future.get().value();
}

TEST(Future, AsyncExceptionVoid) {
  promise<void> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(53ms);
    try {
      throw std::runtime_error("Test");
    }
    catch (const std::runtime_error &) {
      p.set_exception(std::current_exception());
    }
  });

  t.detach();
  EXPECT_THROW(future.get().value(), std::runtime_error);
}

TEST(Future, ContinuationValueBefore) {
  std::atomic_bool done(false);

  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_EQ(result.value(), 42);
    done = true;
  });

  p.set_value(42);

  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationValueAfter) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  p.set_value(42);

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_EQ(result.value(), 42);
    done = true;
  });

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationExceptionBefore) {
  std::atomic_bool done(false);

  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_THROW(result.value(), std::runtime_error);
    done = true;
  });

  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationExceptionAfter) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  try {
    throw std::runtime_error("Test");
  }
  catch (const std::runtime_error &) {
    p.set_exception(std::current_exception());
  }

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_THROW(result.value(), std::runtime_error);
    done = true;
  });

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationValueBeforeAsync) {
  std::atomic_bool done(false);

  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_EQ(result.value(), 41);
    done = true;
  });

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(10ms);
    p.set_value(41);
  });

  while (!done) {
    std::this_thread::yield();
  }

  t.join();
}

TEST(Future, ContinuationValueAfterAsync) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(10ms);
    p.set_value(41);
  });

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_EQ(result.value(), 41);
    done = true;
  });

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }

  t.join();
}

TEST(Future, ContinuationExceptionBeforeAsync) {
  std::atomic_bool done(false);

  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_THROW(result.value(), std::runtime_error);
    done = true;
  });

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(110ms);
    try {
      throw std::runtime_error("Test");
    }
    catch (const std::runtime_error &) {
      p.set_exception(std::current_exception());
    }
  });

  while (!done) {
    std::this_thread::yield();
  }

  t.join();
}

TEST(Future, ContinuationExceptionAfterAsync) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  std::thread t([&]() {
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(110ms);
    try {
      throw std::runtime_error("Test");
    }
    catch (const std::runtime_error &) {
      p.set_exception(std::current_exception());
    }
  });

  /* executes in another thread */
  future.then([&](auto &&result) {
    EXPECT_THROW(result.value(), std::runtime_error);
    done = true;
  });

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }

  t.join();
}

TEST(Future, ContinuationChaining) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([](auto &&result) { EXPECT_EQ(result.value(), 13); })
      .then([&](auto &&result) {
         EXPECT_NO_THROW(result.value());
         done = true;
       });

  p.set_value(13);

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationChainingNonVoid) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([](auto &&result) {
                int v = 0;
                EXPECT_NO_THROW(v = result.value());
                EXPECT_EQ(v, 13);
                return ++v;
              })
      .then([&](auto &&result) {
         int v = 0;
         EXPECT_NO_THROW(v = result.value());
         EXPECT_EQ(v, 14);
         done = true;
       });

  p.set_value(13);

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationChainingThrow) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([](auto &&result) {
                int v = 0;
                EXPECT_NO_THROW(v = result.value());
                EXPECT_EQ(v, 13);
                throw std::runtime_error("My bad.");
                return ++v;
              })
      .then([&](auto &&result) {
         int v = 0;
         EXPECT_THROW(v = result.value(), std::runtime_error);
         done = true;
       });

  p.set_value(13);

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationChainingException) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([](auto &&result) {
                int v = 0;
                EXPECT_THROW(v = result.value(), std::runtime_error);
                throw std::runtime_error("My bad.");
                return ++v;
              })
      .then([&](auto &&result) {
         int v = 0;
         EXPECT_THROW(v = result.value(), std::runtime_error);
         done = true;
       });

  try {
    throw std::runtime_error("My bad.");
  }
  catch (...) {
    p.set_exception(std::current_exception());
  }

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}

TEST(Future, ContinuationChainingExceptionHandle) {
  std::atomic_bool done(false);
  promise<int> p;
  auto future = p.get_future();

  /* executes in another thread */
  future.then([](auto &&result) {
                EXPECT_FALSE(result);
                std::rethrow(result.error());
                return 5;
              })
      .then([&](auto &&result) {
         int v = 0;
         EXPECT_THROW(v = result.value(), std::runtime_error);
         done = true;
       });

  try {
    throw std::runtime_error("My bad.");
  }
  catch (...) {
    p.set_exception(std::current_exception());
  }

  /*
   * libdispatch does not join threads on program exit, so queued functions
   * might get dropped on the floor. Sync explicitly.
   */
  while (!done) {
    std::this_thread::yield();
  }
}



