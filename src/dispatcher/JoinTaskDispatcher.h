#pragma once

#include <tbb/cache_aligned_allocator.h>

#include <atomic>
#include <memory>
#include <mutex>
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
 * \brief This a task dispatcher for two streams used for joins.
 *
 * */

class JoinTaskDispatcher : public ITaskDispatcher {
 private:
  QueryBuffer &m_firstBuffer, &m_secondBuffer;
  WindowDefinition &m_firstWindow, &m_secondWindow;
  TupleSchema *m_firstSchema, *m_secondSchema;
  int m_batchSize;
  int m_firstTupleSize, m_secondTupleSize;

  /* Task Identifier */
  std::atomic<int> m_nextTask;

  /* Pointers */
  long m_firstStartIndex = 0;
  long m_firstNextIndex = 0;
  long m_firstEndIndex = 0;
  long m_firstLastEndIndex = 0;
  long m_firstToProcessCount = 0;
  long m_firstNextTime  = 0;
  long m_firstEndTime  = 0;
  long m_prevFirstFreePointer = 0;

  long m_secondStartIndex = 0;
  long m_secondNextIndex = 0;
  long m_secondLastEndIndex = 0;
  long m_secondEndIndex = 0;
  long m_secondToProcessCount = 0;
  long m_secondNextTime = 0;
  long m_secondEndTime  = 0;
  long m_prevSecondFreePointer = 0;

  long m_mask;
  long m_latencyMark;
  std::mutex m_lock;

  int m_replayBarrier = 0;

  /* Watermark ingestion */
  long m_watermark = LONG_MIN;
  int m_watermarkFrequency = SystemConf::WORKER_THREADS;

  size_t m_assembleId = 0;
  const bool m_symmetric = false;
  const bool m_debug = false;

 public:
  JoinTaskDispatcher(Query &query, QueryBuffer &buffer,
                     QueryBuffer &secondBuffer,
                     bool replayTimestamps = false,
                     bool triggerCheckpoints = false);
  void dispatch(char *data, int length, long latencyMark = -1, long retainMark = 1) override;
  void dispatch(std::shared_ptr<UnboundedQueryBuffer> &data, long latencyMark = -1, long retainMark = -1) override;
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
  ~JoinTaskDispatcher() override;

 private:
  void tryAssembleTask();
  void assembleFirst(long index, int length);
  void assembleSecond(long index, int length);
  void createTask(bool assembledFirst);
  void createSymmetricTask(bool assembledFirst);
  void tryCreateNonProcessingTasks();
  long normaliseIndex(QueryBuffer &buffer, long p, long q);
  long getTimestamp(QueryBuffer &buffer, int index);
  long getSystemTimestamp(QueryBuffer &buffer, int index);
  void setTimestamp(QueryBuffer &buffer, int index, long timestamp);

  struct SerialTask {
    size_t m_id;
    long m_idx;
    int m_length;
    SerialTask() : m_id(0), m_idx(0), m_length(0) {}
  };

  struct CircularTaskList {
    std::vector<SerialTask, tbb::cache_aligned_allocator<SerialTask>> m_buffer;
    int m_size;
    int m_readIdx;
    int m_writeIdx;
    int m_elements = 0;
    CircularTaskList(int size = 0) : m_buffer(size, SerialTask()), m_size(size) {
      m_readIdx = 0;
      m_writeIdx = size - 1;
    }
    void set_capacity(int size) {
      m_buffer.resize(size, SerialTask());
      m_size = size;
      m_readIdx = 0;
      m_writeIdx = size - 1;
    }
    void push_back(size_t id, long idx, int length) {
      if (m_elements == m_size) {
        m_buffer.resize(m_size * 2, SerialTask());
        m_size = 2 * m_size;
      }

      m_writeIdx++;
      if (m_writeIdx == (int) m_buffer.size())
        m_writeIdx = 0;

      m_buffer[m_writeIdx].m_id = id;
      m_buffer[m_writeIdx].m_idx = idx;
      m_buffer[m_writeIdx].m_length = length;

      m_elements++;
    }
    SerialTask *front() {
      if (m_elements > 0)
        return &m_buffer[m_readIdx];
      else
        return nullptr;
        //throw std::runtime_error("error: empty CircularList");
    }
    void pop_front() {
      m_elements--;
      m_readIdx++;
      if (m_readIdx == (int) m_buffer.size())
        m_readIdx = 0;
    }
    int size() { return m_elements; }
    int capacity() { return m_size; }
  };

  // variables for serializing tasks
  size_t m_index = 0;
  std::mutex m_left;
  std::unique_ptr<CircularTaskList> m_leftList;
  std::mutex m_right;
  std::unique_ptr<CircularTaskList> m_rightList;
};