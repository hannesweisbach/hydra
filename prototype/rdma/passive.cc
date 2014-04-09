#include <thread>
#include <chrono>
#include "util/Logger.h"
#include "rdma/RDMAClientSocket.h"

int main(int argc, char * const argv[]) {
  if(argc < 2) {
    log_error() << "Interface required.";
    return -1;
  }

  RDMAClientSocket socket(argv[1], "8042");
  socket.connect();
}

