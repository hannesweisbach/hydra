#set_source_files_properties(client.cpp PROPERTIES COMPILE_FLAGS "-g0")
add_library(hydra server_dht.cpp messages.cpp hopscotch-server.cpp node.cpp client.cpp RDMAAllocator.cpp)
target_link_libraries(hydra logger)

