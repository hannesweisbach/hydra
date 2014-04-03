add_executable(chord_test chord_test.cpp)
target_link_libraries(chord_test ${LIBCITYHASH}  hydra)

add_executable(chord_concurrent chord_concurrent.cpp)
target_link_libraries(chord_concurrent ${LIBCITYHASH}  hydra)

