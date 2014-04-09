#include <thread>
#include <chrono>
#include "util/Logger.h"
#include "rdma/RDMAServerSocket.h"

int main(int argc, char * const argv[]) {
  if(argc < 2) {
    log_error() << "Interface required.";
    return -1;
  }

  RDMAServerSocket socket(argv[1], "8042");
  socket.listen();

  std::this_thread::sleep_for(std::chrono::hours(1));
}
