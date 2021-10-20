#pragma once

#include <chrono>
#include <cfloat>
#include <iomanip>
#include <vector>
#include <atomic>

class SimpleMeasurement {
 private:
  int m_id = 0;
  std::atomic<long> &m_bytesProcessedAtomic;
  double m_Dt;
  double m__1MB_ = 1048576.0;
  long m_bytesProcessed, m__bytesProcessed = 0;
  long m_bytesGenerated, m__bytesGenerated = 0;
  double m_MBpsProcessed, m_MBpsGenerated;
  static long m_sumTuples;
  static int m_measurements;

 public:
  SimpleMeasurement(std::atomic<long> &bytesProcessed);
  std::string getInfo(long delta, int inputTuple = 0, int outputTuple = 0);
  std::string getThroughput(long delta, int inputTuple = 0, int outputTuple = 0);
  ~SimpleMeasurement();
};

/*
 * The ThroughputMonitor runs in the background and collects performance @Measurements that are
 * printed to the user.
 *
 * */

class ThroughputMonitor {
 private:
  int m_counter = 0;
  long m_time, m__time = 0L;
  long m_dt;
  int m_size;
  std::atomic<long> &m_bytesProcessed;
  std::unique_ptr<SimpleMeasurement> m_measurement;
  int m_inputTupleSize, m_outputTupleSize;
  bool m_printInConsole = false;
  std::vector<std::string> m_storedMeasurements;
  size_t m_throughputMonitorInterval = 10; // in ms
  size_t m_duration = 1200;

 public:
  ThroughputMonitor(std::atomic<long> &bytesProcessed, int inputTuple, int outputTuple);
  void operator()();
  ~ThroughputMonitor();
};


