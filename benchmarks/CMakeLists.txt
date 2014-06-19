add_executable(test_ test.cc)
target_link_libraries(test_ logger hydra util ${COMMON_LIBS})
add_executable(hs hs.cc)
target_link_libraries(hs logger hydra util ${COMMON_LIBS})

add_executable(alloc_test AllocatorTest.cpp)
target_link_libraries(alloc_test logger hydra util ${COMMON_LIBS})

add_executable(hashbench hash_bench.cpp)
target_link_libraries(hashbench logger cityhash ${CMAKE_THREAD_LIBS_INIT})
add_custom_command(TARGET hashbench POST_BUILD COMMAND objdump hashbench -hS > ${CMAKE_CURRENT_BINARY_DIR}/hashbench.lss)

add_executable(map_copy map_copy.cc)
target_link_libraries(map_copy  ${COMMON_LIBS} hydra util)

add_subdirectory(dht)
