#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <memory>

#ifdef HAVE_LIBDISPATCH
#include <dispatch/dispatch.h>
#include "util/concurrent.h"
#else
#include "util/WorkerThread.h"
#endif

#include "util/uint128.h"

class Logger {
public:
  enum class severity_level {
    trace,
    debug,
    info,
    warn,
    error,
    fatal
  };

  Logger(const severity_level &level = severity_level::warn);
  Logger(const severity_level &level, const std::string &func, const int line);
  ~Logger();

  template <typename T, typename = typename std::enable_if<
                            !std::is_pointer<T>::value>::type>
  inline Logger &operator<<(const T &arg) {
    if (entry_severity >= severity)
      s << arg;
    return *this;
  }

  template <typename T> inline Logger &operator<<(T *arg) {
    if (entry_severity >= severity) {
      if (arg != nullptr)
        s << arg;
      else
        s << "(nullptr)";
    }
    return *this;
  }

  template <typename T> inline Logger &write(T *arg, std::streamsize size = sizeof(T)) {
    s.write(reinterpret_cast<const char *>(arg), size);
    return *this;
  }

  inline Logger &operator<<(std::ostream &(*pf)(std::ostream &)) {
    if (entry_severity >= severity)
      s << pf;
    return *this;
  }

  inline Logger &operator<<(const std::ios_base &(*pf)(std::ios_base &)) {
    if (entry_severity >= severity)
      s << pf;
    return *this;
  }

  void set_severity(const severity_level &severity) {
    entry_severity = severity;
  }

  static void set_severity(int severity);
  static std::ostream& underlying_stream;

private:
#ifdef HAVE_LIBDISPATCH
  struct Logger_data {
    dispatch_queue_t log_queue;
    Logger_data() : log_queue(dispatch_queue_create("hydra.queue.log", 0)) {}
    ~Logger_data() { dispatch_release(log_queue); }
  };
#else
  struct Logger_data {
    WorkerThread t;
  };
#endif
  static struct Logger_data priv;

  static severity_level severity;
  std::ostringstream s;
  severity_level entry_severity;
};

#define log__(lvl) (Logger((lvl), (__func__), (__LINE__)))

#define log_trace() log__(Logger::severity_level::trace)
#define log_debug() log__(Logger::severity_level::debug)
#define log_info() log__(Logger::severity_level::info)
#define log_warn() log__(Logger::severity_level::warn)
#define log_error() log__(Logger::severity_level::error)
#define log_err() log__(Logger::severity_level::error)
#define log_fatal() log__(Logger::severity_level::fatal)

template <typename T>
void hexdump__(const T *buffer, const size_t size, const char *func,
               const int line) {
  Logger logger(Logger::severity_level::debug, func, line);
  logger << (void *)buffer << " (" << size << "):" << std::hex;
  const volatile uint8_t *raw_ptr =
      reinterpret_cast<const volatile uint8_t *>(buffer);
  for (size_t i = 0; i < size; i++) {
    if ((i & 0xf) == 0)
      logger << std::endl << "  0x" << std::setfill('0') << std::setw(8) << i
             << "  ";
    logger << "0x" << std::setfill('0') << std::setw(2) << (unsigned)raw_ptr[i]
           << " ";
  }
  logger << std::endl << std::dec;
}

template <typename T>
void hexdump__(std::shared_ptr<T> buffer, size_t size = sizeof(T),
               const char *func = "", int line = 0) {
  hexdump__(buffer.get(), size, func, line);
}

template <typename T> void hexdump__(T &buffer, const char *func, int line) {
  hexdump__(&buffer, sizeof(T), func, line);
}

#define log_hexdump(obj) hexdump__((obj), __func__, __LINE__)
#define log_hexdump_ptr(obj, size) hexdump__((obj), (size), __func__, __LINE__)

std::ostream &operator<<(std::ostream &ostream,
                         const Logger::severity_level &severity);

std::ios_base &dec_indent(std::ios_base &os);
std::ios_base &inc_indent(std::ios_base &os);
std::ostream &indent(std::ostream &);

class indent_guard {
public:
  indent_guard(std::ostream &stream) : stream(stream) { stream << inc_indent; }
  ~indent_guard() { stream << dec_indent; }

private:
  std::ostream &stream;
};

