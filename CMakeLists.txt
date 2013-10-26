cmake_minimum_required(VERSION 2.8.9)

project(RDMA-DHT)

FIND_PACKAGE(Boost 1.53.0 COMPONENTS program_options log REQUIRED)

LIST(APPEND SERVER_SOURCES RDMAServer.cpp)

if(UNIX)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -std=c++11")
endif()

add_executable(dht_server ${SERVER_SOURCES})
target_link_libraries(dht_server ibverbs)
target_link_libraries(dht_server rdmacm)
target_link_libraries(dht_server ${Boost_LIBRARIES})
