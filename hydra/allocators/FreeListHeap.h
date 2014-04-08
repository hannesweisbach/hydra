#pragma once
/* -*- C++ -*- */

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
 * @class FreelistHeap
 * @brief Manage freed memory on a linked list.
 * @warning This is for one "size class" only.
 *
 * Note that the linked list is threaded through the freed objects,
 * meaning that such objects must be at least the size of a pointer.
 */

#include <vector>
#include <utility>
#include <assert.h>
#include <mutex>
#include <memory>

#include "util/concurrent.h"

namespace hydra {

template <class SuperHeap> class FreeListHeap : public SuperHeap {
private:
  using freelist_t = std::vector<std::pair<char *, ibv_mr *> >;

  template <typename T> rdma_ptr<T> make_pointer(char *p, ibv_mr *mr) {
    return rdma_ptr<T>(pointer_t<T>(reinterpret_cast<T *>(p), [=](T *p) {
                         std::unique_lock<spinlock> l(*freelist_lock);
                         freelist->emplace_back(reinterpret_cast<char *>(p),
                                                mr);
                       }),
                       mr);
  }

  std::vector<rdma_ptr<char> > allocs;
  std::shared_ptr<freelist_t> freelist;
  std::shared_ptr<spinlock> freelist_lock;

public:
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;

  template <typename... Args, typename = std::enable_if<
                                  !std::is_same<FreeListHeap, Args...>::value> >
  FreeListHeap(Args &&... args)
      : SuperHeap(std::forward<Args>(args)...),
        freelist(std::make_shared<freelist_t>()),
        freelist_lock(std::make_shared<spinlock>()) {}

#if 1
  template <typename T> inline rdma_ptr<T> malloc(const size_t n_elems = 1) {
    const size_t size = n_elems * sizeof(T);
    char *p = nullptr;
    ibv_mr *mr = nullptr;
    {
      std::unique_lock<spinlock> l(*freelist_lock);
      if (!freelist->empty()) {
        p = freelist->back().first;
        mr = freelist->back().second;
        freelist->pop_back();
      }
    }
    if (p == nullptr) {
      auto alloc = SuperHeap::template malloc<char>(size);
      p = alloc.first.get();
      mr = alloc.second;
      allocs.push_back(std::move(alloc));
    }
    return make_pointer<T>(p, mr);
  }
#else
  template <typename T> inline decltype(auto) malloc(size_t size = sizeof(T)) {
    if (!freelist.empty()) {
      while (freelist_lock.test_and_set(std::memory_order_acquire))
        ;
      p = freelist.back().first;
      mr = freelist.back().second;
      freelist.pop_back();
    }
    freelist_lock.clear(std::memory_order_release);
    if (p == nullptr) {
      auto alloc = SuperHeap::template malloc<void>(size);
      p = alloc.first.get();
      mr = alloc.second;
      allocs.push_back(std::move(alloc));
    }
    /* this is more what i had in mind, but std::function needs to be copyable,
     * but our deleter is not, because of the unique_ptr moved into the deleter.
     * therefore, we can't keep the upper level unqiue_ptr within the lambda but
     * in a separate list :/
     * the actual problem is the declaration of the return type, which is
     * std::unique_ptr<value_type, std::function<void(value_type*)>>.
     * we could however instaniate std::unique_type<value_type,
     *decltype(lambda)>,
     * but not declare it as return type. deduction doesn't work, because clang
     * says it's an error if it can't generate debug info for an deduced type
     *(or something):
     *
     * error: debug information for auto is not yet supported
     *
     * we could use a shared_ptr, though.
     */
    value_type *p = ptr.first.get();
    ibv_mr *mr = ptr.second;
    auto deleter = [ =, res = std::move(ptr.first) ](value_type * p) mutable {
      (void)(p);
      freelist.push_back(alloc_type(std::move(res), mr));
    };
    // return alloc_type(memory_type(p, std::move(deleter)),
    // mr_type(ptr.second));

    return std::make_pair(
        std::unique_ptr<value_type, decltype(deleter)>(p, std::move(deleter)),
        mr_type(ptr.second));
  }
#endif
};
}

