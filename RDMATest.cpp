#include <unistd.h>
#include <getopt.h>

#include "hydra/node.h"

#include "Logger.h"
#include "RDMAAllocator.h"

int main(int argc, char *const argv[]) {
  static struct option long_options[] = {
    { "port", required_argument, 0, 'p' },
    { "interface", required_argument, 0, 'i' },
    { "verbosity", optional_argument, 0, 'v' },
    { "connect", required_argument, 0, 'c' },
    { 0, 0, 0, 0 }
  };

#if 1
  std::string host("10.0.0.1");
#else
  std::string host("110.211.55.5");
#endif
  std::string port("8042");

  /* connect to another node */
  std::string remote_node;
  std::string remote_port("8042");
  bool connect_remote = false;

  int verbosity = -1;

  while (1) {
    int option_index = 0;
    int c = getopt_long(argc, argv, "p:i:c:", long_options, &option_index);

    if (c == -1)
      break;
    switch (c) {
    case 'p':
      port = optarg;
      break;
    case 'i':
      host = optarg;
      break;
    case 'v':
      if (optarg) {
        /* TODO: parse optarg for level */
      } else {
        verbosity++;
      }
      break;
    case 'c': {
      connect_remote = true;
      std::string hostport(optarg);
      size_t split = hostport.find(":");
      if (split == std::string::npos) {
        remote_node = hostport;
      } else {
        remote_node = hostport.substr(0, split);
        remote_port = hostport.substr(split + 1);
      }
      log_info() << "Connection to remote node at " << remote_node << ":"
                 << remote_port;
    } break;
    case '?':
    default:
      log_err() << "Unkown option code " << (char)c;
    }
  }

  Logger::set_severity(verbosity);
  hydra::node node(host, port);

  if(connect_remote)
    node.connect(remote_node, remote_port);

  sleep(10 * 3600);
}

