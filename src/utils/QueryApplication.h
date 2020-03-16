#pragma once

#include <vector>

#include "utils/SystemConf.h"

class Query;
class TaskProcessorPool;
class TaskDispatcher;
class PerformanceMonitor;

/*
 * \brief This is the query application that is going to be executed once
 * the @setup is called first and the @processData function gets invoked.
 *
 * A query application can have multiple @Queries.
 * */

class QueryApplication {
 private:
  int m_numOfThreads;
  std::vector<std::shared_ptr<Query>> m_queries;
  int m_numOfQueries;
  /*
   * At the top level, the input stream will be will
   * be dispatched to the most upstream queries
   */
  int m_numberOfUpstreamQueries;
  std::shared_ptr<TaskQueue> m_queue;
  std::shared_ptr<TaskProcessorPool> m_workerPool;
  std::vector<std::shared_ptr<TaskDispatcher>> m_dispatchers;
  std::unique_ptr<PerformanceMonitor> m_performanceMonitor;
  std::thread m_performanceMonitorThread;

  void setDispatcher(std::shared_ptr<TaskDispatcher> dispatcher);

 public:
  QueryApplication(std::vector<std::shared_ptr<Query>> &queries);
  void processData(std::vector<char> &values, long latencyMark = -1);
  void setup();
  std::shared_ptr<TaskQueue> getTaskQueue();
  int getTaskQueueSize();
  std::vector<std::shared_ptr<Query>> getQueries();
  std::shared_ptr<TaskProcessorPool> getTaskProcessorPool();
  int numberOfQueries();
  int numberOfUpStreamQueries();
};