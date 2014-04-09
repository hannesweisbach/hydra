add_executable(active active.cc)
target_link_libraries(active ${COMMON_LIBS} logger util)

add_executable(passive passive.cc)
target_link_libraries(passive ${COMMON_LIBS} logger util)


