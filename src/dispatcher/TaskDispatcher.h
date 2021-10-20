#pragma once

#include <memory>
#include <stdexcept>

#include "dispatcher/ITaskDispatcher.h"

class ResultHandler;
class Query;
class WindowDefinition;
class TupleSchema;
class Task;
class QueryBuffer;
class OperatorCode;
class FileBackedCheckpointCoordinator;
class UnboundedQueryBuffer;

/*
 * \brief This a task dispatcher for a single stream.
 *
 * For time-based windows, it keeps track of the timestamps when creating a task
 * to find gaps between consecutive batches and inform the worker about it. In
 * the NUMA-aware context, it assigns to each task information regarding the
 * data locality.
 *
 * If the replay mode is on by setting true replayTimestamps, the bundle and
 * batch size should be defined appropriately for it to work.
 *
 * */

class TaskDispatcher : public ITaskDispatcher {
 private:
  QueryBuffer &m_buffer;
  WindowDefinition &m_window;
  TupleSchema *m_schema;
  int m_batchSize;
  int m_tupleSize;

  /* Task Identifier */
  std::atomic<int> m_nextTask;

  /* Pointers */
  long m_f = 0;
  long m_mask;
  long m_latencyMark;
  long m_accumulated = 0;
  long m_thisBatchStartPointer;
  long m_nextBatchEndPointer;
  bool m_replayTimestamps;
  long m_offset = 0;
  long m_step = -1;

  int m_replayBarrier = 0;

  /* Watermark ingestion */
  long m_watermark = LONG_MIN;
  int m_watermarkFrequency = SystemConf::WORKER_THREADS;

  long m_recoveryOffset = 0;
  const bool m_debug = false;

#if defined(HAVE_SHARED)
  std::unique_ptr<boost::interprocess::managed_shared_memory> m_segment;
  long *m_o;
  long *m_s;
#endif

 public:
  TaskDispatcher(Query &query, QueryBuffer &buffer,
                 bool replayTimestamps = false,
                 bool triggerCheckpoints = false);
  void dispatch(char *data, int length, long latencyMark = -1, long retainMark = -1) override;
  void dispatch(std::shared_ptr<UnboundedQueryBuffer> &data, long latencyMark = -1, long retainMark = -1) override;
  void dispatch(void *data, int length, long latencyMark = -1, long retainMark = -1) override;
  void dispatchToFirstStream(char *data, int length, long latencyMark) override;
  void dispatchToSecondStream(char *data, int length, long latencyMark) override;
  bool tryDispatchOrCreateTask(char *data, int length, long latencyMark = -1, long retain = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  bool tryDispatch(char *data, int length, long latencyMark = -1, long retain = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  bool tryDispatchToFirstStream(char *data, int length, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  bool tryDispatchToSecondStream(char *data, int length, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  bool tryDispatchSerialToFirstStream(char *data, int length, size_t id, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  bool tryDispatchSerialToSecondStream(char *data, int length, size_t id, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override;
  void tryToConsume() override;
  void recover() override;
  QueryBuffer *getBuffer() override;
  QueryBuffer *getFirstBuffer() override;
  QueryBuffer *getSecondBuffer() override;
  void setTaskQueue(std::shared_ptr<TaskQueue> queue) override;
  long getBytesGenerated() override;
  void setLastTaskId(int taskId);
  void setCheckpointCoordinator(FileBackedCheckpointCoordinator *coordinator);
  int getTaskNumber() override;
  void setTaskNumber(int taskId) override;
  void setStepAndOffset(long step, long offset) override;
  void createMergeTasks(bool flag) override;
  ~TaskDispatcher() override;

 private:
  void assemble(long index, int length);
  void newTaskFor(long p, long q, long free, long b_, long _d);
  void tryCreateNonProcessingTasks();
  long getTimestamp(int index);
  long getSystemTimestamp(int index);
  void setTimestamp(int index, long timestamp);
};