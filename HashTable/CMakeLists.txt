add_executable(HashTest TestHash.cpp)
target_link_libraries(HashTest gtest gtest_main cityhash)
#add_test(HashTest HashTest)

add_executable(HopscotchTest HopscotchTest.cpp)
target_link_libraries(HopscotchTest gtest gtest_main cityhash)
add_test(HopscotchTest HopscotchTest)
