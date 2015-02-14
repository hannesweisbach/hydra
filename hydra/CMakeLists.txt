#set_source_files_properties(client.cpp PROPERTIES COMPILE_FLAGS "-g0")
#set_source_files_properties(node.cpp PROPERTIES COMPILE_FLAGS "-cc1 -fdump-record-layouts")
#set_source_files_properties(node.cpp PROPERTIES COMPILE_FLAGS "-E")

add_library(hydra client.cc chord.cc types.cc hopscotch-server.cpp node.cpp passive.cpp RDMAAllocator.cpp fixed_network.c++ network.c++)
target_link_libraries(hydra logger util rdma ${LIBCAPNP} dhtproto future)

add_subdirectory(protocol)
