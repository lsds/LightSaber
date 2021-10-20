#pragma once

#include <stack>
#include <vector>

#include "utils/SystemConf.h"

class Query;
class TaskProcessorPool;
class ITaskDispatcher;
class CompressionMonitor;
class PerformanceMonitor;
class QueueIoHandler;
template <class H> class FileSystemDisk;
template <class H> class FileSystemFile;
class FileBackedCheckpointCoordinator;
class BlockManager;
class UnboundedQueryBuffer;

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
  size_t m_taskQueueCapacity;
  std::shared_ptr<TaskQueue> m_queue;
  std::shared_ptr<TaskProcessorPool> m_workerPool;
  std::vector<std::shared_ptr<ITaskDispatcher>> m_dispatchers;
  std::unique_ptr<CompressionMonitor> m_compressionMonitor;
  std::unique_ptr<PerformanceMonitor> m_performanceMonitor;
  std::thread m_compressionMonitorThread, m_performanceMonitorThread;

  /*
   * Variables used for checkpointing
   * */
  bool m_checkpointEnabled;
  typedef QueueIoHandler adapter_t;
  typedef FileSystemDisk<adapter_t> disk_t;
  typedef FileSystemFile<adapter_t> file_t;
  std::shared_ptr<disk_t> m_filesystem;
  std::unique_ptr<FileBackedCheckpointCoordinator> m_checkpointCoordinator;
  std::thread m_checkpointCoordinatorThread;

  std::atomic<bool> m_clearFiles;
  std::shared_ptr<BlockManager> m_fileStore;

  std::vector<int> m_rates;

  void setDispatcher(std::shared_ptr<ITaskDispatcher> dispatcher);

  void topologicalSort(int q, std::vector<bool> &visited, std::stack<int> &stack);

 public:
  explicit QueryApplication(std::vector<std::shared_ptr<Query>> &queries, bool checkpointEnabled = false, bool clearFiles = true);
  void processData(std::vector<char> &values, long latencyMark = -1, long retainMark = -1);
  void processData(std::shared_ptr<UnboundedQueryBuffer> &values, long latencyMark = -1, long retainMark = -1);
  void processData(void *values, int length, long latencyMark = -1, long retainMark = -1);
  void processFirstStream(std::vector<char> &values, long latencyMark = -1);
  void processSecondStream(std::vector<char> &values, long latencyMark = -1);
  bool tryProcessData(std::vector<char> &values, long latencyMark);
  void recoverData();
  void setup();
  void setupRates(std::vector<int> &rates);
  std::shared_ptr<TaskQueue> getTaskQueue();
  size_t getTaskQueueSize();
  size_t getTaskQueueCapacity();
  std::vector<std::shared_ptr<Query>> getQueries();
  std::shared_ptr<TaskProcessorPool> getTaskProcessorPool();
  FileBackedCheckpointCoordinator *getCheckpointCoordinator();
  long getTimestampReference();
  long getLastTimestamp();
  int numberOfQueries();
  int numberOfUpstreamQueries();
  ~QueryApplication();
};