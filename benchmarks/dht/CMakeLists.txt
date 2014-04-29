add_executable(load load.cc)
target_link_libraries(load ${COMMON_LIBS} hydra)

add_executable(put_loop put_loop.cc)
target_link_libraries(put_loop ${COMMON_LIBS} hydra)
