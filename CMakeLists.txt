cmake_minimum_required(VERSION 2.8.9)

project(hydra)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

add_subdirectory(gtest-1.7.0)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

if(${BUILD_SHARED_LIBS})
  message(STATUS "Shared linking enabled")
  #  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIE -fPIC")
  #set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -fPIE -fPIC")
else()
  message(STATUS "Static linking enabled")
  #  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--eh-frame-hdr ")
endif()

enable_testing(true)
execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE OUTPUT_STRIP_TRAILING_WHITESPACE)
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS NDEBUG)
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS LEVEL1_DCACHE_LINESIZE=${LEVEL1_DCACHE_LINESIZE})


if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast -Wno-shadow -Wno-missing-prototypes")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-padded -Wno-sign-conversion -Wno-weak-vtables")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftemplate-backtrace-limit=0")
  #  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftrapv -fsanitize=undefined-trap -fsanitize-undefined-trap-on-error")
  #SET(CMAKE_CXX_FALGS "${CMAKE_CXX_FLAGS} -no-integrated-as")
  #set_source_files_properties(hydra/allocators/FreeListHeap.h PROPERTIES COMPILE_FLAGS -g0)
  #  SET(CMAKE_LINK_FLAGS "${CMAKE_LINK_FLAGS} -stdlib=libc++ -nodefaultlibs -lc++ -lc++abi -lm -lc -lgcc_s -lgcc")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
else()
  message(ERROR "Unsupported compiler.")
endif()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -pthread")

#TODO: llok for librdmacm, libibverbs ...
FIND_PACKAGE(Threads)
find_library(LIBDISPATCH dispatch "/usr/local/")
find_library(LIBCITYHASH cityhash "/usr/local")
find_library(LIBCAPNP    capnp "/usr/local")
find_library(LIBKJ       kj "/usr/local")

#set(LIBDISPATCH "dispatch-NOTFOUND")
if(${LIBDISPATCH} MATCHES "-NOTFOUND")
  message(STATUS "libdispatch not found. Will build without it")
  set(HAVE_LIBDISPATCH OFF)
else()
  message(STATUS "Found libdispatch.")
  add_definitions(-DHAVE_LIBDISPATCH)
  set(HAVE_LIBDISPATCH ON)
endif()

if(${LIBCITYHASH} MATCHES "NOTFOUND")
  message(FATAL_ERROR "libcityhash not found.")
endif()

if(${LIBCAPNP} MATCHES "NOTFOUND")
  message(FATAL_ERROR "libcapnp not found.")
endif()

if(${LIBKJ} MATCHES "NOTFOUND")
  message(FATAL_ERROR "libkj not found.")
endif()

SET(LIBCAPNP "${LIBCAPNP};${LIBKJ}")

include_directories(.)
include_directories(util rdma hydra)
include_directories("/usr/local/include")
#generated headers. because cmake refuses to propagate includes to parent scope.
#TODO: propagate include file from hydra/protocol and target_include_dir it.
include_directories(${CMAKE_CURRENT_BINARY_DIR}/hydra/protocol)
LIST(APPEND COMMON_LIBS rdma ${LIBCITYHASH} ${LIBDISPATCH} ${CMAKE_THREAD_LIBS_INIT} trace)

add_subdirectory(util)
add_subdirectory(rdma)
add_subdirectory(hydra)
add_subdirectory(prototype)

LIST(APPEND SERVER_SOURCES RDMAServer.cpp ${COMMON_SOURCES})
LIST(APPEND CLIENT_SOURCES RDMAClient.cpp ${COMMON_SOURCES})


add_subdirectory(benchmarks)

add_executable(serversocket RDMATest.cpp)
target_link_libraries(serversocket logger hydra util ${COMMON_LIBS})
set(PROFILER ON)
if(${PROFILER})
  target_link_libraries(serversocket profiler)
  add_definitions(-DPROFILER)
endif()
add_test(RDMAServerSocket serversocket)
add_custom_command(TARGET serversocket POST_BUILD
    COMMAND sudo setcap cap_sys_resource+ep serversocket
    COMMENT "Setting capability CAP_SYS_RESOURCE for target serversocket"
    WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH}
)

#add_executable(dht_server ${SERVER_SOURCES})
#target_link_libraries(dht_server ${COMMON_LIBS} logger util)
#add_custom_command(TARGET dht_server POST_BUILD
#    COMMAND sudo setcap cap_sys_resource+ep dht_server
#    COMMENT "Setting capability CAP_SYS_RESOURCE for target dht_server"
#    WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH}
#)


add_executable(dht_client ${CLIENT_SOURCES})
target_link_libraries(dht_client hydra util ${COMMON_LIBS})
add_custom_command(TARGET dht_client POST_BUILD
    COMMAND sudo setcap cap_sys_resource+ep dht_client
    COMMENT "Setting capability CAP_SYS_RESOURCE for target dht_client"
    WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH}
)
set_source_files_properties(RDMAClient.cpp RDMAServer.cpp PROPERTIES COMPILE_DEFINITIONS "_GNU_SOURCE")

if(${HAVE_LIBDISPATCH})
add_executable(recursive_async recursive_async.cpp)
target_link_libraries(recursive_async util ${LIBDISPATCH} logger)
endif()

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/check-modprobe-capability.sh RESULT_VARIABLE NO_MODPROBE_CAP)
if(${NO_MODPROBE_CAP})
  message(STATUS "modprobe doesn't have cap_sys_module capability.")
  message(STATUS "set it with setcap cap_sys_module+ep `readlink -f $(which modprobe)`")
endif()
