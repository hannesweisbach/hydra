#pragma once

#include <sys/epoll.h>

namespace hydra {
namespace util {
class epoll {
  int efd;

public:
  epoll();
  ~epoll();
  void add(int fd, epoll_event *event);
  void del(int fd);
  void mod(int fd, epoll_event *event);
  int wait(epoll_event *events, int maxevents, int timeout);
};
}
}

