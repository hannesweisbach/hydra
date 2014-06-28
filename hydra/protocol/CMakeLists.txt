find_program(CAPNP NAMES capnp PATHS /usr/local/bin)
if(${CAPNP} MATCHES "NOTFOUND")
  message(FATAL_ERROR "Please install capnp.")
endif()

SET(SRC ${CMAKE_CURRENT_SOURCE_DIR})
SET(BIN ${CMAKE_CURRENT_BINARY_DIR})
add_custom_command(OUTPUT ${BIN}/dht.capnp.h ${BIN}/dht.capnp.c++
                   COMMAND ${CAPNP} compile -oc++:${BIN} --src-prefix=${SRC} ${SRC}/dht.capnp
                   DEPENDS ${SRC}/dht.capnp)
add_library(dhtproto STATIC dht.capnp.c++)
