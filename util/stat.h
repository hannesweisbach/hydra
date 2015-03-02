#pragma once

#include <chrono>
#include <vector>
#include <ostream>
#include <algorithm>
#include <iterator>
#include <string>
#include <sstream>

using rep_type = std::chrono::nanoseconds::rep;

class ecdf {
  std::vector<rep_type> times;

  using time_point = decltype(std::chrono::high_resolution_clock::now());
  time_point start_;

public:
  ecdf() { times.reserve(1000 * 1000); }

  void start() { start_ = std::chrono::high_resolution_clock::now(); }
  void end() {
    using namespace std::chrono;
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start_);
    times.push_back(duration.count());
  }
  void add(rep_type data) { times.push_back(data); }

  friend std::ostream &operator<<(std::ostream &s, ecdf &rhs) {
    if (rhs.times.empty()) {
      s << "No data points" << std::endl;
      return s;
    }
    std::sort(std::begin(rhs.times), std::end(rhs.times));
    const size_t size = rhs.times.size() - 1;
    const size_t ylines = 20;
    const float step = 1.0f / 20;
    auto xmin = step;
    auto xmax = 1 - step;
    auto min_idx = static_cast<size_t>(size * xmin);
    auto max_idx = static_cast<size_t>(size * xmax);
    auto ymin = rhs.times.at(min_idx);
    auto ymax = rhs.times.at(max_idx);
    auto yrange = ymax - ymin;
    float ydiv = yrange / (ylines - 1);
    s << rhs.times.size() << " data points" << std::endl;
    s << " 5th percentile: " << rhs.times.at(static_cast<size_t>(0.05 * size))
      << std::endl;
    s << "25th percentile: " << rhs.times.at(static_cast<size_t>(0.25 * size))
      << std::endl;
    s << "50th percentile: " << rhs.times.at(static_cast<size_t>(0.50 * size))
      << std::endl;
    s << "75th percentile: " << rhs.times.at(static_cast<size_t>(0.75 * size))
      << std::endl;
    s << "95th percentile: " << rhs.times.at(static_cast<size_t>(0.95 * size))
      << std::endl;
    std::vector<std::ostringstream> lines(ylines);
    {
      auto pos = ymax;
      for (auto &line : lines) {
        line << pos << " ";
        pos -= ydiv;
      }
    }
    
    for (auto percentile = step; percentile < 1 - step; percentile += step) {
      auto idx = static_cast<size_t>(percentile * size);
      auto y = rhs.times.at(idx);
      bool found = false;
      auto pos = ymax;
      std::for_each(std::begin(lines), std::end(lines) - 1,
                    [&found, &pos, y, ydiv](auto &&line) {
        if (!found && y > pos) {
          line << "*";
          found = true;
        } else {
          line << " ";
        }
        pos -= ydiv;
      });
      lines.back() << (!found ? "*" : " ");
    }

    for (const auto &line : lines) {
      s << line.str() << std::endl;
    }

    return s;
  }
};

