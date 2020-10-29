#pragma once

#include <stdexcept>
#include <memory>

class WindowBatch;

enum TaskType : uint8_t { PROCESS, MERGE};

static inline const std::string taskTypeToString(TaskType v) {
  switch (v) {
    case PROCESS: return "PROCESS";
    case MERGE: return "MERGE";
    default:throw std::runtime_error("error: unknown aggregation type");
  }
}

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
  TaskType m_type;

 public:
  Task();
  Task(int taskId, std::shared_ptr<WindowBatch> batch, TaskType type = TaskType::PROCESS);
  void set(int taskId, std::shared_ptr<WindowBatch> batch, TaskType type = TaskType::PROCESS);
  int run(int pid);
  void outputWindowBatchResult(std::shared_ptr<WindowBatch> result);
  int getTaskId();
  int getQueryId();
  int getNumaNodeId();
  ~Task();
};