# hydra
My stab at an RDMA DHT

There has been a number of designs and implementations using Infiniband to improve the performance of DHTs/Key-Value stores. Hydra is my contribution, although at the time of its release all of Hydra's features have been used in other KV-stores. The only thing setting Hydra apart is the (to my knowledge) unique combination of design ideas.

Hydra is based upon the usual OpenFabrics Alliance stack of libibverbs/librdma_cm as user space libraries and the supporting kernel modules of ib_ipoib, ib_uverbs, ib_umad, rdma_ucm, and ib_mthca. Hydra requires a C++14-compliant standard library. At the time of this writing only libc++ fulfils this requirement. Using clang++ to compile Hydra is recommended.
