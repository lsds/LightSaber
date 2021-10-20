#pragma once

class QueryBuffer;
class ITaskDispatcher;
class ResultHandler;
class LatencyMonitor;

#include <string>

/*
 * The measurements are printed on screen based on a predefined frequency value.
 * They contain information about the throughput, latency, number of tasks in the
 * queues and the sizes of buffer pools.
 *
 * */

class Measurement {
 private:
  int m_id;
  ITaskDispatcher *m_dispatcher;
  QueryBuffer *m_firstBuffer, *m_secondBuffer = nullptr;
  LatencyMonitor *m_monitor;
  double m_Dt;
  double m__1MB_ = 1048576.0;
  long m_bytesProcessed, m__bytesProcessed = 0;
  long m_bytesGenerated, m__bytesGenerated = 0;
  double m_MBpsProcessed, m_MBpsGenerated;
  //long m_time, m__time = 0;
  static long m_sumTuples;
  static int m_measurements;

 public:
  Measurement(int id = -1, ITaskDispatcher *dispatcher = nullptr, LatencyMonitor *monitor = nullptr);
  void stop();
  std::string getInfo(long delta, int inputTuple = 0, int outputTuple = 0);
  ~Measurement();
};


