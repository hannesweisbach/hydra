#pragma once

#include <atomic>
#include <map>
#include <lockguard>

#include "util/utils.h"

/* TODO additional parameters:
 * - increment
 * - minimal size
 *   increment == minimal size?
 */
template <typename SuperHeap> class alignas(64) CoalesceHeap : public SuperHeap {
  template <typename T>
  using pointer_t = typename SuperHeap::template pointer_t<T>;
  template <typename T>
  using rdma_ptr = typename SuperHeap::template rdma_ptr<T>;
  
  struct Chunk {
    Chunk *prev;
    Chunk *next;
    void *ptr;
    size_t size;
    bool free;
  };

  void expand(size_t size = chunkSize) {
    arenas.push_back(SuperHeap::template malloc<char>(size));
    remaining = size;
    current = 0;
  }
  std::vector<rdma_ptr<char> > arenas;
  std::list<Chunk> freelist;
  hydra::spinlock listlock;

  ListNode * last;

  static coalesce(ListNode * p, ListNode * n) {
    assert(p->next == n);
    assert(n->prev == p);

    p->size += n->size;

    p->next = n->next;
    p->next->prev = p;
  }

public:
  template <typename... Args>
  CoalesceHeap(Args &&... args)
      : SuperHeap(std::forward<Args>(args)...) {}

  template <typename T> rdma_ptr<T> malloc(const size_t n_elems = 1) {
    const size_t size = n_elems * sizeof(T);
#if 0
    if(size < minimalSize)
      size = minimalSize;
    else
      size =  (size + x -1) & ~(x-1);
#endif
    std::list<Chunk> out;
    {
      std::unique_lock<hydra::spinlock> l(listlock);
      auto free = find_if(std::begin(freelist), std::end(freelist),
                          [=]() { return size <= blocksize });
      if (free != std::end(freelist)) {
        /* splice out free block */
        out.splice(free, freelist);
      }
    }
    Chunk current_chunk = {last, nullptr, nullptr, 0, false};

    if(out.empty()) {
      /* allocate new + split if neccesary */
      expand(std::max(size, chunkSize));
      current_chunk.ptr = ;
      current_chunk.size = size;
      out.emplace_back(prev, next, ptr, size - chunkSize, true);
    } else {
      Chunk c = out.front();
      out.clear();
      if(c.size > size + minSize) {
        out.emplace_back(prev, next, ptr, size - chunkSize, true);
      } else {

      }

    }

    /* splice out into the freelist */
    if(!out.empty()) {
      std::unique_lock<hydra::spinlock> l(listlock);
      freelist.splice(out);
    }


    auto node = std::make_shared<ListNode>
        last, nullptr, arenas.back().first.get(), size, true, ATOMIC_FLAG_INIT);
    remaining -= size;
    rdma_ptr<T> ret(
        pointer_t<T>(reinterpret_cast<T *>(arenas.back().first.get() + current),
                     [](T *p) {
          if (node.prev->next == p && !node.prev->in_use) {
            /* remove from map/vector */
            {
            }
            /*
             * coalesce
             * add to map/vector
             */
            coalesce(node.prev, node);
          } else if (node.next.prev == p && !node.next.in_use) {
            coalesce(node, node.next);
          }
        }),
        arenas.back().second);
    current += size;

    return ret;
  }
};
