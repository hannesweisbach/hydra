#include <utility>

#include <unistd.h>
#include <getopt.h>

#include "hydra/node.h"

#include "Logger.h"
#include "RDMAAllocator.h"

#ifdef PROFILER
#include <gperftools/profiler.h>
#endif

#if 0
#include <gperftools/heap-profiler.h>
#endif

int main(int argc, char *const argv[]) {
  static struct option long_options[] = {
    { "port", required_argument, 0, 'p' },
    { "interface", required_argument, 0, 'i' },
    { "verbosity", optional_argument, 0, 'v' },
    { "connect", required_argument, 0, 'c' },
    { 0, 0, 0, 0 }
  };

  using conn_t = std::pair<std::vector<std::string>, std::string>;
#if 1
  conn_t host({}, "8042");
#else
  conn_t host({"110.211.55.5"}, "8042");
#endif

  using address_t = std::pair<std::string, std::string>;
  address_t remote({}, "8042");
  bool connect_remote = false;

  int verbosity = -1;

  while (1) {
    int option_index = 0;
    int c = getopt_long(argc, argv, "p:i:c:", long_options, &option_index);

    if (c == -1)
      break;
    switch (c) {
    case 'p':
      host.second = optarg;
      break;
    case 'i':
      host.first.push_back(optarg);
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
        remote.first = hostport;
      } else {
        remote.first = hostport.substr(0, split);
        remote.second = hostport.substr(split + 1);
      }
      log_info() << "Connection to remote node at " << remote.first << ":"
                 << remote.second;
    } break;
    case '?':
    default:
      log_err() << "Unkown option code " << (char)c;
    }
  }

  if (host.first.empty())
    host.first.push_back("10.0.0.1");

  Logger::set_severity(verbosity);
  hydra::node node(host.first, host.second);

  if(connect_remote)
    node.join(remote.first, remote.second);
  else
    node.join(host.first[0], host.second);

#ifdef PROFILER
  ProfilerStart("./server.prof");
#endif
  //HeapProfilerStart("./server-heap.profile");

  double load = node.load();

  while(1) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    if(load != node.load()) {
      node.dump();
      load = node.load();
      log_info() << "Load factor: " << node.load();
    }

#ifdef PROFILER
    ProfilerFlush();
#endif
    //HeapProfilerDump("foo");
  }

#ifdef PROFILER
  ProfilerStop();
#endif
}

