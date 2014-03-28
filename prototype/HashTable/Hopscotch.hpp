#pragma once
#include <vector>
#include <iterator>
#include <limits>
#include <iostream>
#include <assert.h>

#include <gtest/gtest_prod.h>

#include "HashTable.hpp"
#include "Hash.hpp"
#include "hydra/hash.h"

#include "modulo-iterator.h"

template <typename Key, typename Data>
class Hopscotch : HashTable<Key, Data> {
  FRIEND_TEST(HopscotchTest, SimpleContains);
  FRIEND_TEST(HopscotchTest, HopContains);
  friend class HopscotchFixture;
  struct Bucket {
    Key key;
    Data data;
    uint32_t hop;
  };
  std::vector<Bucket> table;
  const unsigned int hop_range;
  const float growth_factor;

  template <typename _Predicate>
  size_t iterate_mod(size_t start, size_t end, _Predicate pred) {
    size_t mod = table.size();
    for(size_t i = start; i != end; i = (i+1) % mod) {
      if(pred(table[i]))
        return i;
    }
    return end;
  }

  template <typename _Predicate>
  const Bucket& find_hop_if(size_t index, uint32_t hop, _Predicate pred) {
    for (size_t i = index; hop; i = (i + 1) % table.size(), hop >>= 1) {
      if (hop & 1) {
        if (pred(table[i]))
          return table[i];
      }
    }
    return table[index];
  }

  size_t can_move_forward(const decltype(table.begin()) b) {
    ssize_t should = hash(b->key) % table.size();
    ssize_t is = std::distance(table.begin(), b);
    ssize_t move = is - should;
    return (move < 0) ? 0 : move;
  }

  size_t find_next_moveable(size_t from) {
    size_t size = table.size();
    size_t start = (from - (hop_range - 1)) % size;
#if 0
    size_t item = iterate_mod(start, from, [](Bucket b) {
      uint32_t hop = b.hop;
      for(size_t d = 0; hop; d++, hop>>=1) {
        if(hop & 1) {
          if(((from - i) % size) > d)
            return (i + d) % size;
        }
      }

    });
#endif
    for(size_t i = start; i != from; i = (i+1) % size) {
      uint32_t hop = table[i].hop;
      for(size_t d = 0; hop; d++, hop>>=1) {
        if(hop & 1) {
          //if(from - i > d)
          if(((from - i) % size) > d)
            return (i + d) % size;
        }
      }
    }
    return 0;
  }

  size_t move_next_free_to(size_t needed, size_t free) {
    dump();
    size_t movable = find_next_moveable(free);
    if(movable == 0)
      return 0;

    std::cout << "Moving " << movable << " to " << free << std::endl;
    table[free].key = table[movable].key;
    table[free].data = table[movable].data;
    /* fixup hop info in home-bucket of movable */
    size_t home = ::hydra::hash(table[movable].key) % table.size();
   
    std::cout << home << " " << movable << " " << free << std::endl;
    
    size_t old_distance = (movable - home) % table.size();
    size_t new_distance = (free - home) % table.size();

    std::cout << new_distance << " " << old_distance << std::endl; 
    assert(old_distance < hop_range);
    assert(new_distance < hop_range);

    table[home].hop |=  (1 << new_distance);
    table[home].hop &= ~(1 << old_distance);
    
    table[movable].key = 0;


    size_t remaining = (movable - needed) % table.size();
    if(remaining < hop_range) {
      std::cout << "Remaining " << remaining << " needed are within hop range" << std::endl;
      return home;
    } else {
      std::cout << "Need " << remaining << " more slots from " << needed << " to " << movable  << std::endl;
      dump();
      return move_next_free_to(needed, movable);
    }
  }

  void resize() {
    std::vector<Bucket> old_table = table;
    table.clear();
    table.resize(old_table.size() * growth_factor);

    for (auto b : old_table) {
      add(b.key, b.data);
    }
  }

  public:
    Hopscotch(unsigned int initial_capacity = 16, unsigned int hop_range = 4, const float growth_factor = 1.3f)
        : table(initial_capacity), hop_range(hop_range), growth_factor(growth_factor) {}

    bool contains(const Key& key) const override {
      const size_t index = ::hydra::hash(key) % table.size();
      uint32_t hop = table[index].hop;
      
      for(size_t i = 0; hop; i++, hop>>=1) {
        if(hop & 1) {
          const Key& current_key = table[(index + i) % table.size()].key;
          if(current_key == key)
            return true;
        }
      }

      return false;
    }

    Data add(const Key& key, const Data& data) override {
      size_t index = ::hydra::hash(key) % table.size();
      auto& bucket = table[index];

      //TODO: add contains();
      // try hop region
      for(size_t i = 0; i < hop_range; ++i) {
        size_t hood_index = (index + i) % table.size();
        auto& hood_bucket = table[hood_index];
        if(hood_bucket.key == 0 || hood_bucket.key == key) {
          /* add in the hood */
          hood_bucket.key = key;
          hood_bucket.data = data;
          /* update hop info */
          assert(i < hop_range);
          bucket.hop |= (1 << i);
          return data;
        }
      }

      // try moving some stuff
      size_t size = table.size();
      size_t i = (index + hop_range) % size;
      for(; i != index; i = (i+1) % size) {
        if(table[i].key == 0)
          break;
      }
      
      std::cout << "Need " << index << " next free is " << i << std::endl;
      if(i != index) {
        size_t free = move_next_free_to(index, i);
          
        std::cout << "movable found " << std::boolalpha << free << std::endl;
        if(free)
          return add(key, data);
      } else {
        std::cout << "no next free" << std::endl;;
      }
      
      std::cout << "No more moves left. Need to resize(). Aborting." << std::endl;
      dump();
      assert(false);
      return 0;
#if 0
      resize();
      return add(key, data);
#endif
    }
    void dump() {
      std::cout << std::hex;
      for(Bucket b : table)
        std::cout << b.key << " ";
      std::cout << std::endl;
      for(Bucket b : table)
        std::cout << b.data << " ";
      std::cout << std::endl;
      for(Bucket b : table) 
        std::cout << std::hex << b.hop << " ";
      std::cout << std::dec << std::endl;  
    }
    
    Data get(const Key& key) const override { 
      const size_t index = ::hydra::hash(key) % table.size();
      uint32_t hop = table[index].hop;
      
      for(size_t i = 0; hop; i++, hop>>=1) {
        if(hop & 1) {
          const Bucket& bucket = table[(index + i) % table.size()];
          if(bucket.key == key)
            return bucket.data;
        }
      }

      return 0; 
    }
    
    Data remove(const Key& key) override {
      const size_t index = ::hydra::hash(key) % table.size();
      uint32_t hop = table[index].hop;
      
      for(size_t i = 0; hop; i++, hop>>=1) {
        if(hop & 1) {
          Bucket& bucket = table[(index + i) % table.size()];
          if(bucket.key == key) {
            bucket.key = 0;
            return bucket.data;
          }
        }
      }

      return 0; 
    }
};
