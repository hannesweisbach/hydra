cmake_minimum_required(2.8.9)

project(RDMA-DHT)

APPEND_LIST(SERVER_SOURCES RDMAServer.cpp)

add_executable(dht_server ${SERVER_SOURCES})
target_link_library(dht_server ibverbs)
target_link_library(dht_server rdmacm)
