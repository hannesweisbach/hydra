add_subdirectory(overlay)

add_executable(udl udl.cc)
target_link_libraries(udl hydra)

add_executable(lambda lambda.cc)

add_subdirectory(rdma)
add_subdirectory(dispatch)
#add_subdirectory(HashTable)
