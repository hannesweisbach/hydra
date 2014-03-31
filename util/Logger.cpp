#include <iostream>

#include "Logger.h"

Logger::Logger_data Logger::priv;

std::ostream &operator<<(std::ostream &ostream,
                         const Logger::severity_level& severity) {
  switch (severity) {
  case Logger::severity_level::trace:
    return ostream << "[t]";
  case Logger::severity_level::debug:
    return ostream << "[d]";
  case Logger::severity_level::info:
    return ostream << "[i]";
  case Logger::severity_level::warn:
    return ostream << "[w]";
  case Logger::severity_level::error:
    return ostream << "[e]";
  case Logger::severity_level::fatal:
    return ostream << "[f]";
  }
  return ostream;
}

Logger::Logger(const severity_level &level) : entry_severity(level) {
  *this << level;
}

Logger::Logger(const severity_level &level, const std::string &func,
               const int line)
    : entry_severity(level) {
  std::ostringstream s;
  s << func << "(" << line << "): ";
  *this << level << " " << std::setw(22) << s.str();
}

Logger::~Logger() {
  if (entry_severity >= severity) {
    s << std::endl;
    auto task = [](std::string &&s) { std::cerr << s; };
#ifdef HAVE_LIBDISPATCH
    hydra::async(priv.log_queue, task, s.str()).wait();
#else
    priv.t.send(task, s.str());
#endif
  }
}

void Logger::set_severity(int severity) {
  Logger::severity_level sev;
  switch (severity) {
  case 0:
    sev = Logger::severity_level::fatal;
    break;
  case 1:
    sev = Logger::severity_level::error;
    break;
  case 2:
    sev = Logger::severity_level::warn;
    break;
  case 3:
    sev = Logger::severity_level::info;
    break;
  case 4:
    sev = Logger::severity_level::debug;
    break;
  default:
    sev = Logger::severity_level::trace;
    break;
  }
  Logger::severity = sev;
}

Logger::severity_level Logger::severity = Logger::severity_level::trace;

static int index = std::ios_base::xalloc();

std::ios_base &inc_indent(std::ios_base &os) {
  os.iword(index)++;
  return os;
}

std::ios_base &dec_indent(std::ios_base &os) {
  os.iword(index)--;
  return os;
}

std::ostream &indent(std::ostream &os) {
  auto indent = os.iword(index);
  if (indent) {
    std::streamsize old_width = os.width(indent * 2);
    char fill = os.fill(' ');
    os << " ";
    os.width(old_width);
    os.fill(fill);
  }
  return os;
}

