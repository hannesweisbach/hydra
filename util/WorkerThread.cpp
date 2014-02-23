#include <future>
#include <iostream>

#include "WorkerThread.h"

WorkerThread::WorkerThread() : thread(&WorkerThread::process_work, this) {}
WorkerThread::~WorkerThread() {
  send([=]() { done = true; });
  if(thread.joinable())
    thread.join();
}

void
WorkerThread::process_work() {
  while(!done) {
    std::function<void()> work_item;
    {
      std::unique_lock<std::mutex> lock(list_lock);
      while(queue.size() == 0) {
        empty.wait(lock);
      }
      work_item = queue.front();
      queue.pop_front();
    }
    work_item();
  };
}
