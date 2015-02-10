add_subdirectory(overlay)

add_executable(udl udl.cc)
target_link_libraries(udl hydra)

add_executable(lambda lambda.cc)

add_executable(capnp_test capnp.cc)
target_link_libraries(capnp_test ${LIBCAPNP} dhtproto)

SET(DISABLED_WARNINGS "-Wno-global-constructors")
SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} -Wno-undef")
SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} -Wno-deprecated")
SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} -Wno-missing-noreturn")
SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} -Wno-used-but-marked-unused")
SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} -Wno-shift-sign-overflow")
#SET(DISABLED_WARNINGS "${DISABLED_WARNINGS} ")
set_property(SOURCE future.c++ PROPERTY COMPILE_FLAGS ${DISABLED_WARNINGS})
add_executable(future_test future.c++)
target_link_libraries(future_test gtest_main future)

add_subdirectory(rdma)
#add_subdirectory(HashTable)
