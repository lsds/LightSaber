#pragma once

#include <cfloat>
#include <string>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <cmath>
#include <atomic>
#include <iostream>
#include <vector>

class QueryBuffer;

/*
 * This class is used to measure the end-to-end latency of a pipeline.
 * In the current implementation, the system has to inject a timestamp to the
 * first element of each batch by multiplexing the actual tuple's timestamp and the latency
 * timestamp. This value get propagated through the execution graph and it is finally compared
 * with the time that the result is emitted.
 *
 * */

class LatencyMonitor {
 private:
  long m_count;
  double m_min, m_max, m_avg;
  long m_timestampReference = 0;
  double m_latency;
  std::atomic<bool> m_active;
  std::vector<double> m_measurements;

 public:
  explicit LatencyMonitor(long timeReference);
  void disable();
  std::string toString();
  void monitor(QueryBuffer &buffer, long latencyMark);
  void stop();

 private:
  double evaluateSorted(const double p);
};