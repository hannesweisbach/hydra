add_executable(load load.cc)
target_link_libraries(load ${COMMON_LIBS} hydra)

add_executable(put_loop put_loop.cc)
target_link_libraries(put_loop ${COMMON_LIBS} hydra)

add_executable(put_loop_inline put_loop_inline.cc)
target_link_libraries(put_loop_inline ${COMMON_LIBS} hydra)

add_executable(put_loop_future put_loop_future.c++)
target_link_libraries(put_loop_future ${COMMON_LIBS} hydra)

add_executable(get get.c++)
target_link_libraries(get ${COMMON_LIBS} hydra)

add_executable(get_loop get_loop.cc)
target_link_libraries(get_loop ${COMMON_LIBS} hydra)

add_executable(mixed mixed.c++)
target_link_libraries(mixed ${COMMON_LIBS} hydra)
