#include <iostream>

#include "util/demangle.h"


int main(int argc, char * const argv[]) {
  if(argc < 2) {
    std::cout << "No name given." << std::endl;
    return -1;
  }

  for(int i = 0; i < argc; i++) {
    std::cout << hydra::util::demangle(argv[i]) << std::endl;
  }
  
  return 0;
}

