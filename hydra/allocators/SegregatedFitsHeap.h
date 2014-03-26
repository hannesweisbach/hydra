// -*- C++ -*-
#pragma once
/*

  Heap Layers: An Extensible Memory Allocation Infrastructure

  Copyright (C) 2000-2012 by Emery Berger
  http://www.cs.umass.edu/~emery
  emery@cs.umass.edu

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

/**
 * @file segheap.h
 * @brief Definition of SegHeap.
 */

/**
 * @class SegHeap
 * @brief A segregated-fits collection of (homogeneous) heaps.
 * @author Emery Berger
 *
 * Note that one extra heap is used for objects that are "too big".
 *
 * @param NumBins The number of bins (subheaps).
 * @param getSizeClass Function to compute size class from size.
 * @param getClassMaxSize Function to compute the largest size for a given size
 *class.
 * @param LittleHeap The subheap class.
 * @param BigHeap The parent class, used for "big" objects.
 *
 * Example:<BR>
 * <TT>
 *  int myFunc (size_t sz); // The size-to-class function.<BR>
 *  size_t myFunc2 (int); // The class-to-size function.<P>
 *  // The heap. Use freelists for these small objects,<BR>
 *  // but defer to malloc for large objects.<P>
 *
 * SegHeap<4, myFunc, myFunc2, freelistHeap<MallocHeap>, MallocHeap> mySegHeap;
 * </TT>
 **/

#include <vector>
#include <functional>
#include <algorithm>
#include <iterator>

#include <assert.h>
#include "util/utils.h"

namespace hydra {

template <class BinHeap, class SuperHeap>
class SegregatedFitsHeap : public SuperHeap {
  static const size_t shift =
      hydra::util::static_log2<std::numeric_limits<size_t>::digits>::value;
  enum {
    SHIFTS_PER_ULONG =
        hydra::util::static_log2<std::numeric_limits<size_t>::digits>::value
  };
  inline size_t sizeClass2Entry(size_t i) {
    size_t entry = i >> shift;
    assert(entry < entries);
    return entry;
  }

  static inline size_t sizeClass2Bit(size_t i) {
    unsigned long bit = ((1UL << ((i) & ((1UL << shift) - 1))));
    return bit;
  }
#if 0

  inline int get_binmap(int i) const {
    return binmap[i >> SHIFTS_PER_ULONG] & idx2bit(i);
  }

  inline void mark_bin(int i) { binmap[i >> SHIFTS_PER_ULONG] |= idx2bit(i); }

  inline void unmark_bin(int i) {
    binmap[i >> SHIFTS_PER_ULONG] &= ~(idx2bit(i));
  }

  const size_t _maxObjectSize;
#endif
  size_t entries;
  std::vector<size_t> binmap;
  std::function<size_t(size_t)> size2Class;
  size_t maxSize;
  std::vector<BinHeap> binHeap;

public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;

  template <typename... Args>
  SegregatedFitsHeap(size_t numBins, std::function<size_t(size_t)> size2Class,
                     Args &&... args)
      : SuperHeap(std::forward<Args>(args)...),
        /* rounded up division of numBins / bits per size_t */
        entries((numBins + std::numeric_limits<size_t>::digits - 1) /
                std::numeric_limits<size_t>::digits),
        binmap(entries, 0), size2Class(size2Class) {
    for (size_t i = 0, lastSize = 0; size2Class(i) < numBins; i++) {
#ifndef NDEBUG
      if(size2Class(i) != size2Class(i + 1)) {
        log_info() << "Bucket " << size2Class(i) << " from " << lastSize << " to " << i << " (" << i - lastSize << ")";
        lastSize = i + 1;
      }
#else
      (void)(lastSize);
#endif
      maxSize = i;
    }
    for (size_t i = 0; i < numBins; i++) {
      // TODO this breaks for rvalue arguments ... add static assert or
      // something
      binHeap.emplace_back(std::forward<Args>(args)...);
    }
    log_info() << numBins << " " << maxSize;
  }

  SegregatedFitsHeap(SegregatedFitsHeap &&other)
      : SuperHeap(std::forward<SuperHeap>(other)), entries(other.entries),
        binmap(std::move(other.binmap)),
        size2Class(std::move(other.size2Class)), maxSize(other.maxSize),
        binHeap(std::move(other.binHeap)) {}

  SegregatedFitsHeap &operator=(SegregatedFitsHeap &&other) {
    std::swap(entries, other.entries);
    std::swap(binmap, other.binmap);
    std::swap(size2Class, other.size2Class);
    std::swap(maxSize, other.maxSize);
    std::swap(binHeap, other.binHeap);

    return *this;
  }

  template <typename T>
  inline rdma_ptr<T> malloc(const size_t n_elems = 1) {
    const size_t size = n_elems * sizeof(T);
    if (size > maxSize) {
      return SuperHeap::template malloc<T>(n_elems);
    } else {
      return binHeap[size2Class(size)].template malloc<T>(n_elems);
    }
#if 0

    {
      const int sc = getSizeClass(sz);
      assert(sc >= 0);
      assert(sc < NumBins);
      int idx = sc;
      int block = idx2block(idx);
      unsigned long map = binmap[block];
      unsigned long bit = idx2bit(idx);

      for (;;) {
        if (bit > map || bit == 0) {
          do {
            if (++block >= NUM_ULONGS) {
              goto GET_MEMORY;
              // return bigheap.malloc (sz);
            }
          } while ((map = binmap[block]) == 0);

          idx = block << SHIFTS_PER_ULONG;
          bit = 1;
        }

        while ((bit & map) == 0) {
          bit <<= 1;
          assert(bit != 0);
          idx++;
        }

        assert(idx < NumBins);
        ptr = myLittleHeap[idx].malloc(sz);

        if (ptr == NULL) {
          binmap[block] = map &= ~bit; // Write through
          idx++;
          bit <<= 1;
        } else {
          _memoryHeld -= sz;
          return ptr;
        }
      }
    }

  GET_MEMORY:
    if (ptr == NULL) {
      // There was no free memory in any of the bins.
      // Get some memory.
      ptr = bigheap.malloc(sz);
    }

    return ptr;
#endif
  }
#if 0
  inline void free(void *ptr) {
    // printf ("Free: %x (%d bytes)\n", ptr, getSize(ptr));
    const size_t objectSize = getSize(ptr); // was bigheap.getSize(ptr)
    if (objectSize > _maxObjectSize) {
      bigheap.free(ptr);
    } else {
      int objectSizeClass = getSizeClass(objectSize);
      assert(objectSizeClass >= 0);
      assert(objectSizeClass < NumBins);
      // Put the freed object into the right sizeclass heap.
      assert(getClassMaxSize(objectSizeClass) >= objectSize);
#if 1
      while (getClassMaxSize(objectSizeClass) > objectSize) {
        objectSizeClass--;
      }
#endif
      assert(getClassMaxSize(objectSizeClass) <= objectSize);
      if (objectSizeClass > 0) {
        assert(objectSize >= getClassMaxSize(objectSizeClass - 1));
      }

      myLittleHeap[objectSizeClass].free(ptr);
      mark_bin(objectSizeClass);
      _memoryHeld += objectSize;
    }
  }
#endif
};
}

