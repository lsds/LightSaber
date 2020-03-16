#pragma once

#include <stdexcept>
#include <memory>

class WindowBatch;

/*
 * \brief This class represent a task in LightSaber and has a pointer to @WindowBatch of data.
 *
 * The @m_numaNodeId indicates a preference for NUMA-aware scheduling.
 *
 * */

class Task {
 private:
  std::shared_ptr<WindowBatch> m_batch;
  int m_taskId;
  int m_queryId;
  int m_numaNodeId;

 public:
  Task();
  Task(int taskId, std::shared_ptr<WindowBatch> batch);
  void set(int taskId, std::shared_ptr<WindowBatch> batch);
  int run(int pid);
  void outputWindowBatchResult(std::shared_ptr<WindowBatch> result);
  int getTaskId();
  int getQueryId();
  int getNumaNodeId();
  ~Task();
};