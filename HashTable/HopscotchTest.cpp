#include <vector>
#include <algorithm>
#include <limits>
#include <cstring>

#include <gtest/gtest.h>

#include "Hopscotch.hpp"

class IteratorIndexTest : public ::testing::TestWithParam<int> {
};

class HopscotchFixture : public ::testing::Test {

public:

  template <typename Key, typename Data>
  size_t size(const Hopscotch<Key, Data> &dht) {
    return dht.table.size();
  }

  template <typename Key, typename Data>
  Key getKeyAt(const Hopscotch<Key, Data> &dht, size_t i) {
    return dht.table[i].key;
  }

  template <typename Key, typename Data>
  Data getDataAt(const Hopscotch<Key, Data> &dht, size_t i) {
    return dht.table[i].data;
  }

  template <typename Key, typename Data>
  uint32_t getHopAt(const Hopscotch<Key, Data> &dht, size_t i) {
    return dht.table[i].hop;
  }
};

TEST_P(IteratorIndexTest, IteratorTest) {
  std::vector<int> v;
  for(int i = 0; i < 5; ++i)
    v.push_back(i);

  size_t size = v.size();

  size_t offset = (size_t)GetParam();
  auto start = v.begin() + ((offset + 1) % size);
  ASSERT_EQ(std::distance(v.begin(), start), (ptrdiff_t)((offset + 1) % size));
  
  auto end = v.begin() + (offset % size);
  ASSERT_EQ(std::distance(v.begin(), end), (ptrdiff_t)(offset % size));
}

INSTANTIATE_TEST_CASE_P(IteratorTestInstance, IteratorIndexTest, ::testing::Range(0, 10));
#if 0
TEST(HopscotchTest, SimpleContains) {
  const size_t size = 16;
  Hopscotch<uint32_t, uint32_t> hopscotch(size);

  ASSERT_EQ(hopscotch.table.size(), size);

  auto h = ::hydra::hash(1);
  size_t i = h % hopscotch.table.size();
  Hopscotch<uint32_t,uint32_t>::Bucket bucket = {1, (uint32_t)i, 1};

  hopscotch.table[i] = bucket;

  ASSERT_TRUE(hopscotch.contains(1));
}
#endif

TEST_F(HopscotchFixture, SimpleAdd) {
  Hopscotch<uint32_t, uint32_t> dht;

  const uint32_t key = 54;
  const uint32_t data = 23;

  dht.add(key, data);

  const size_t h = ::hydra::hash(key);
  const size_t i = h % size(dht);

  ASSERT_EQ(getKeyAt(dht, i), key);
  ASSERT_EQ(getDataAt(dht, i), data);
  ASSERT_EQ(getHopAt(dht, i), (uint32_t)1);
  ASSERT_TRUE(dht.contains(key));
}

TEST_F(HopscotchFixture, DoubleAdd) {
  Hopscotch<uint32_t, uint32_t> dht;

  const uint32_t key1 = 1;
  const size_t idx1 = ::hydra::hash(key1) % size(dht);
  uint32_t key2;
  size_t idx2;

  /* find a second key, which maps to the same table index */
  for(uint32_t k = 2; k; k++) {
    idx2 = ::hydra::hash(k) % size(dht);
    if(idx2 == idx1) {
      key2 = k;
      break;
    }
  }
    
  ASSERT_EQ(idx1, idx2);
  ASSERT_TRUE(key2 != key1);

  const uint32_t data1 = 23;
  const uint32_t data2 = 33;

  dht.add(key1, data1);
  ASSERT_TRUE(dht.contains(key1));
  ASSERT_EQ(getKeyAt(dht, idx1), key1);
  ASSERT_EQ(getDataAt(dht, idx1), data1);
  ASSERT_EQ(getHopAt(dht, idx1), (uint32_t)1);

  dht.add(key2, data2);
  ASSERT_TRUE(dht.contains(key2));
  ASSERT_EQ(getKeyAt(dht, idx2), key1);
  ASSERT_EQ(getDataAt(dht, idx2), data1);
  ASSERT_EQ(getHopAt(dht, idx2), (uint32_t)3);
 
  const size_t next_idx = (idx2 + 1) % size(dht);
  ASSERT_EQ(getKeyAt(dht, next_idx), key2);
  ASSERT_EQ(getDataAt(dht, next_idx), data2);
  ASSERT_EQ(getHopAt(dht, next_idx), (uint32_t)0);

}

#if 0
TEST_F(HopscotchFixture, Fill) {
  Hopscotch<uint32_t, uint32_t> dht;


  for(uint32_t i = 31; i < 44; i++) {
    dht.add(i, i);
  }
  
  dht.dump();
  std::cout << (size_t)(::hydra::hash(2) % 16) << std::endl;

  dht.add(2,2);
  dht.dump();
}

TEST_F(HopscotchFixture, Fill2) {
  Hopscotch<uint32_t, uint32_t> dht;


  for(uint32_t i = 1; i; i++) {
    dht.add(i, i);
  }
  
  dht.dump();
  std::cout << (size_t)(hash(2) % 16) << std::endl;

  dht.add(2,2);
  dht.dump();
}
#endif

#if 0
TEST(HopscotchTest, HopContains) {
  const size_t size = 16;
  std::vector<Hopscotch<uint32_t, uint32_t>::Bucket> buckets(size);

  for(size_t key = 0; key < (1ULL << std::numeric_limits<uint32_t>::digits); ++key) {
    auto h = ::hydra::hash(key);
    size_t i = h % size;
    buckets[i] = {(uint32_t)key, (uint32_t)key, 0};
    //auto it = find_if(buckets.begin(), buckets.end(), [](Hopscotch<uint32_t, uint32_t>::Bucket b) { return b.key == 0; });
    auto it = find_if(buckets.begin(), buckets.end(), [](decltype(buckets[0]) b) { return b.key == 0; });
    if(it == buckets.end())
      break;
  }

  auto duplicates(buckets);
  for(auto& b : duplicates) {
    b.data++;
  }

  for(auto b : buckets) {
    std::cout << b.key << " ";
  }
  std::cout << std::endl;
  
  for(auto b : buckets) {
    std::cout << b.data << " ";
  }
  std::cout << std::endl;
  
  for(auto b : duplicates) {
    std::cout << b.data << " ";
  }
  std::cout << std::endl;
  
  
  Hopscotch<uint32_t, uint32_t> hopscotch(size);

  ASSERT_EQ(hopscotch.table.size(), size);

  Hopscotch<uint32_t,uint32_t>::Bucket bucket = {1, 1, 1};
  auto h = hash(1);
  size_t i = h % hopscotch.table.size();

  hopscotch.table[i] = bucket;

  ASSERT_TRUE(hopscotch.contains(1));
}
#endif
