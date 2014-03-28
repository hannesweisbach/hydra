#include <algorithm>
#include <vector>
#include <iostream>
#include <limits>

#include <gtest/gtest.h>

#include <boost/integer.hpp>


#include "hydra/hash.h"
#include "Hash.hpp"

template <typename T> static bool done(const std::vector<T> &v) {
//  auto it = find_if(v.begin(), v.end(), [](T val) { return val == 0; });
//  return it == v.end();
  for(size_t i = 1; i < v.size(); i += 2)
    if(v[i] == 0)
      return false;
  return true;
}

template <typename T> static bool only_odd(const std::vector<T> &v) {
//  auto it = find_if(v.begin(), v.end(), [](T val) { return val == 0; });
//  return it == v.end();
  for(size_t i = 0; i < v.size(); i += 2)
    if(v[i] == 1)
      return false;
  return true;
}

TEST(HashTest, ARange) {
  const size_t power = 4;
  std::vector<unsigned char> as(1 << std::numeric_limits<unsigned char>::digits);

  while(!done(as)) {
    Hash<unsigned char> hash(power);
    ASSERT_TRUE(only_odd(as));
    as[hash.a] = 1;
  }
}

TEST(HashTest, BRange) {
  const size_t power = 4;
  const size_t b_bits = std::numeric_limits<unsigned char>::digits - power;
  std::vector<unsigned char> bs(1 << b_bits);

  while(!done(bs)) {
    Hash<unsigned char> hash(power);
    size_t b = hash.b;
    ASSERT_LT(b, bs.size());
    bs[b] = 1;
  }
}

TEST(HashTest, VRange) {
  __int128_t a = 1;
  __int128_t b = 4;
  const size_t power = 4;
  const size_t b_bits = std::numeric_limits<unsigned char>::digits - power;
  std::vector<unsigned char> hashs(1 << b_bits);
  
  Hash<unsigned char> hash(power);
  //Hash<unsigned char> _hash(9);

  for(size_t i = 0; i <= std::numeric_limits<unsigned char>::max(); i++) {
    unsigned char h = hash.hash(i);
    ASSERT_LT(h, hashs.size()) << i << "a: " << (int)hash.a << ", b: " << (int) hash.b << ", bits: " << hash.wM;
    hashs[h]++;
  }

  for(auto c : hashs) {
    EXPECT_EQ(c, 16);
    //std::cout << (int)c << " ";
  }
  //std::cout << std::endl;

  //boost::int_t<65>::least my_var;

  std::cout << (int)(a + b) << " " << std::numeric_limits<__int128_t>::digits << " " << std::numeric_limits<__int128_t>::is_specialized;
}

TEST(CityHash, Compile) {
  char foo[] = "test";
  ::hydra::hash(foo, sizeof(foo));


}
