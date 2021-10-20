#pragma once

#include <chrono>
#include <cfloat>
#include <iomanip>
#include <vector>

class QueryApplication;
class Measurement;

/*
 * The PerformanceMonitor runs in the background and collects performance @Measurements that are
 * printed to the user.
 *
 * */

class PerformanceMonitor {
 private:
  int m_counter = 0;
  long m_time, m__time = 0L;
  long m_dt;
  QueryApplication &m_application;
  int m_size;
  std::vector<Measurement *> m_measurements;
  std::chrono::high_resolution_clock::time_point m_t1;

 public:
  PerformanceMonitor(QueryApplication &application);
  void operator()();
  ~PerformanceMonitor();
};


