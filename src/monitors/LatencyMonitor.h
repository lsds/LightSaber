#pragma once

#include <utils/SystemConf.h>

#include <atomic>
#include <cfloat>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <string>
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
  long m_lastTimestamp = 0;
  double m_latency;
  std::atomic<bool> m_active;
  std::vector<double> m_measurements;
  const std::string m_fileName =  SystemConf::FILE_ROOT_PATH + "/scabbard/latency-metrics";
  const std::string m_fileName2 =  SystemConf::FILE_ROOT_PATH + "/scabbard/latency-metrics-2";
  int m_fd, m_fd2;
  bool m_clearFiles;
  long m_restartReference = 0;
  double m_remainingTime = 0.;

 public:
  explicit LatencyMonitor(long timeReference, bool clearFiles = true);
  void disable();
  std::string toString();
  void monitor(QueryBuffer &buffer, long latencyMark);
  [[nodiscard]] long getTimestampReference() const;
  [[nodiscard]] long getLastTimestamp() const;
  void stop();

 private:
  double evaluateSorted(double p);
};