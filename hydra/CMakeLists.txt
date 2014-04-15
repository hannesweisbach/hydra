#set_source_files_properties(client.cpp PROPERTIES COMPILE_FLAGS "-g0")
#set_source_files_properties(node.cpp PROPERTIES COMPILE_FLAGS "-cc1 -fdump-record-layouts")
#set_source_files_properties(node.cpp PROPERTIES COMPILE_FLAGS "-E")

add_library(hydra client.cc chord.cc types.cc server_dht.cpp messages.cpp hopscotch-server.cpp node.cpp passive.cpp RDMAAllocator.cpp)
target_link_libraries(hydra logger util rdma)

