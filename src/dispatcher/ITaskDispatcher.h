#pragma once

#include <memory>
#include <stdexcept>

#include "utils/SystemConf.h"

class Query;
class Task;
class QueryBuffer;
class FileBackedCheckpointCoordinator;
struct LineageGraph;
class UnboundedQueryBuffer;

/*
 * \brief Base class for task dispatchers.
 *
 * */

class ITaskDispatcher {
 protected:
  std::shared_ptr<TaskQueue> m_workerQueue;
  Query &m_parent;

  /* Recovery and Checkpoints */
  std::atomic<bool> m_createMergeTasks = true;
  bool m_startingFromRecovery = true;
  int m_lastTaskId = 0;
  bool m_triggerCheckpoints;
  std::atomic<bool> m_checkpointFinished = false;
  FileBackedCheckpointCoordinator *m_coordinator;
  std::thread m_coordinationTimerThread;
  std::atomic<int> m_checkpointCounter = 0;
  bool m_parallelInsertion = false;

  friend class FileBackedCheckpointCoordinator;

  friend class FileBackedCheckpointCoordinator;

 public:
  ITaskDispatcher(Query &query, bool triggerCheckpoints = false);
  virtual void dispatch(char *data, int length, long latencyMark = -1, long retainMark = -1) = 0;
  virtual void dispatch(std::shared_ptr<UnboundedQueryBuffer> &data, long latencyMark = -1, long retainMark = -1) = 0;
  virtual void dispatch(void *data, int length, long latencyMark = -1, long retainMark = -1) {
    throw std::runtime_error("error: dispatching receive_element_t not implemented");
  }
  virtual void dispatchToFirstStream(char *data, int length, long latencyMark) = 0;
  virtual void dispatchToSecondStream(char *data, int length, long latencyMark) = 0;
  virtual bool tryDispatchOrCreateTask(char *data, int length, long latencyMark = -1, long retain = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual bool tryDispatch(char *data, int length, long latencyMark = -1, long retain = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual bool tryDispatchToFirstStream(char *data, int length, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual bool tryDispatchToSecondStream(char *data, int length, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual bool tryDispatchSerialToFirstStream(char *data, int length, size_t id, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual bool tryDispatchSerialToSecondStream(char *data, int length, size_t id, long latencyMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;
  virtual void tryToConsume() = 0;
  virtual void recover() = 0;
  virtual QueryBuffer *getBuffer() = 0;
  virtual QueryBuffer *getFirstBuffer() = 0;
  virtual QueryBuffer *getSecondBuffer() = 0;
  virtual long getBytesGenerated() = 0;
  virtual void setTaskQueue(std::shared_ptr<TaskQueue> queue) = 0;
  virtual int getTaskNumber() = 0;
  virtual void setTaskNumber(int taskId) = 0;
  virtual void setStepAndOffset(long step, long offset) = 0;
  virtual ~ITaskDispatcher() {}

  virtual void createMergeTasks(bool flag) = 0;
};