#pragma once

#include <random>
#include <chrono>
#include <limits>
#include <assert.h>
#include <iostream>

#include <gtest/gtest_prod.h>

template <typename T>
class Hash {
  size_t wM;
  T a;
  T b;
  FRIEND_TEST(HashTest, ARange);
  FRIEND_TEST(HashTest, BRange);
  FRIEND_TEST(HashTest, VRange);

public:
  Hash(int power)
      : wM(std::numeric_limits<T>::digits - power),
        a(static_cast<T>((std::mt19937_64(std::chrono::system_clock::now()
                                              .time_since_epoch()
                                              .count())() *
                          2) -
                         1)),
        b(static_cast<T>(
            std::mt19937_64(
                std::chrono::system_clock::now().time_since_epoch().count())() &
            ((1 << wM) - 1))) {
    assert(std::numeric_limits<T>::digits > power);
  }
  T hash(const T x) { return (T)(a * x + b) >> wM; }
};

