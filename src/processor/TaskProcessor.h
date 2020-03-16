#pragma once

#include <math.h>
#include <chrono>
#include <atomic>
#include <memory>

#include "utils/SystemConf.h"

class TaskFactory;

/*
 * \brief This class implements the logic of a worker thread.
 *
 * */

class TaskProcessor {
 private:
  std::shared_ptr<TaskQueue> m_queue;
  int m_pid;
  int m_numaNodeId;

  /* Measurements */
  //std::atomic<long> *tasksProcessed;

  /* Latency measurements (timing queue's poll method) */
  bool m_monitor = false;
  long m_count = 0L;
  long m_start;
  double m_dt;
  double m__m, m_m, m__s, m_s;
  double m_avg = 0, m_std = 0;

 public:
  TaskProcessor(int pid, std::shared_ptr<TaskQueue> queue);
  void operator()();
  void enableMonitoring();
  long getProcessedTasks(int qid);
  double mean();
  double stdv();
  ~TaskProcessor();
};
