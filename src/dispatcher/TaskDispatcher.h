#pragma once

#include <memory>
#include <stdexcept>

#include "utils/SystemConf.h"

class ResultHandler;
class Query;
class WindowDefinition;
class TupleSchema;
class Task;
class QueryBuffer;
class OperatorCode;

/*
 * \brief This a task dispatcher for a single stream.
 *
 * For time-based windows, it keeps track of the timestamps when creating a task to
 * find gaps between consecutive batches and inform the worker about it. In the NUMA-aware context,
 * it assigns to each task information regarding the data locality.
 *
 * If the replay mode is on by setting true replayTimestamps, the bundle and batch size
 * should be defined appropriately for it to work.
 *
 * */

class TaskDispatcher {
 private:
  std::shared_ptr<TaskQueue> m_workerQueue;
  Query &m_parent;
  QueryBuffer &m_buffer;
  WindowDefinition &m_window;
  TupleSchema *m_schema;
  int m_batchSize;
  int m_tupleSize;

  /* Task Identifier */
  int m_nextTask;

  /* Pointers */
  long m_f;
  long m_mask;
  long m_latencyMark;
  long m_accumulated = 0;
  long m_thisBatchStartPointer;
  long m_nextBatchEndPointer;
  bool m_replayTimestamps;
  long m_offset = 0;
  long m_step = -1;

  int m_replayBarrier = 0;
 public:
  TaskDispatcher(Query &query, QueryBuffer &buffer, bool replayTimestamps = false);
  void dispatch(char *data, int length, long latencyMark = -1);
  bool tryDispatch(char *data, int length, long latencyMark = -1);
  QueryBuffer *getBuffer();
  void setTaskQueue(std::shared_ptr<TaskQueue> queue);
  long getBytesGenerated();
  ~TaskDispatcher();

 private:
  void assemble(long index, int length);
  void newTaskFor(long p, long q, long free, long b_, long _d);
  int getTaskNumber();
  long getTimestamp(int index);
  long getSystemTimestamp(int index);
  void setTimestamp(int index, long timestamp);
};