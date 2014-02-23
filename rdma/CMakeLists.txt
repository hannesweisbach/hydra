add_library(rdma STATIC RDMAWrapper.cpp RDMAServerSocket.cpp RDMAClientSocket.cpp)
target_link_libraries(rdma ibverbs rdmacm logger workerthread)


