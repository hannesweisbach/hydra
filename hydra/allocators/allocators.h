#pragma once

#include "RDMAAllocator.h"

#include "allocators/PerThreadAllocator.h"
#include "allocators/ZoneHeap.h"
#include "allocators/ThreadSafeHeap.h"
#include "allocators/FreeListHeap.h"
#include "allocators/SegregatedFitsHeap.h"

#include "util/utils.h"

using default_heap_t = hydra::ThreadSafeHeap<hydra::SegregatedFitsHeap<
    hydra::FreeListHeap<
        hydra::ZoneHeap<RdmaHeap<ibv_access::READ>, 1024 * 1024 * 16> >,
    hydra::ZoneHeap<RdmaHeap<ibv_access::READ>, 16 * 1024 * 1024> > >;

static inline size_t default_size_classes(size_t size) {
  if (size == 0)
    return 0;
  if (size <= 128) // 16 bins
    return ((size + (8 - 1)) & ~(8 - 1)) / 8 - 1;
  else if (size <= 4096) // 31 bins
    return ((size + (128 - 1)) & ~(128 - 1)) / 128 + 14;
  else
    return 47 + hydra::util::log2(size - 1) -
           hydra::util::static_log2<4096>::value;
}
