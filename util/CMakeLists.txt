add_library(workerthread STATIC WorkerThread.cpp)
add_library(uint128 STATIC uint128.cpp)
target_link_libraries(workerthread)
add_library(logger STATIC Logger.cpp)
add_library(demangle STATIC demangle.cc)

add_library(epoll STATIC epoll.cc)

add_executable(demangler demangler.cc)
target_link_libraries(demangler demangle)

add_library(future STATIC future.c++)
target_link_libraries(future dispatch util)

if(${HAVE_LIBDISPATCH})
  target_link_libraries(logger ${LIBDISPATCH} uint128)
else()
  target_link_libraries(logger workerthread uint128)
endif()
add_library(util STATIC utils.cc)
target_link_libraries(util logger)
