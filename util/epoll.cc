#include <unistd.h>

#include "util/epoll.h"
#include "util/exception.h"
#include "util/Logger.h"

namespace hydra {
namespace util {
epoll::epoll() : efd(::epoll_create(1)) {
  if (efd < 0)
    throw_errno();
}

epoll::~epoll() { ::close(efd); }
void epoll::add(int fd, epoll_event *event) {
  check_zero(::epoll_ctl(efd, EPOLL_CTL_ADD, fd, event));
}
void epoll::del(int fd) {
  check_zero(::epoll_ctl(efd, EPOLL_CTL_DEL, fd, nullptr));
}
void epoll::mod(int fd, epoll_event *event) {
  check_zero(::epoll_ctl(efd, EPOLL_CTL_MOD, fd, event));
}
int epoll::wait(epoll_event *events, int maxevents, int timeout) {
  for (;;) {
    int ret = ::epoll_wait(efd, events, maxevents, timeout);
    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        throw_errno();
      }
    }
    return ret;
  }
}
}
}

