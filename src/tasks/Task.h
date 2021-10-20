#pragma once

#include <memory>
#include <stdexcept>

class WindowBatch;
struct LineageGraph;
class UnboundedQueryBuffer;

enum TaskType : uint8_t { PROCESS, ONLY_PROCESS, MERGE, FORWARD, MERGE_FORWARD, CHECKPOINT, RECOVER, INSERT };

static inline const std::string taskTypeToString(TaskType v) {
  switch (v) {
    case PROCESS: return "PROCESS";
    case ONLY_PROCESS: return "ONLY_PROCESS";
    case MERGE: return "MERGE";
    case FORWARD: return "FORWARD";
    case MERGE_FORWARD: return "MERGE_FORWARD";
    case CHECKPOINT: return "CHECKPOINT";
    case RECOVER: return "RECOVER";
    case INSERT: return "INSERT";
    default:throw std::runtime_error("error: unknown aggregation type");
  }
}

/*
 * \brief This class represents a task in LightSaber and has a pointer to @WindowBatch of data.
 *
 * The @m_numaNodeId indicates a preference for NUMA-aware scheduling.
 *
 * */

class Task {
 private:
  std::shared_ptr<WindowBatch> m_leftBatch, m_rightBatch;
  int m_taskId;
  int m_queryId;
  int m_numaNodeId;
  TaskType m_type;

  // used for insertion
  std::shared_ptr<UnboundedQueryBuffer> m_qBuffer;
  char *m_buffer;
  void *m_rdmaBuffer;
  size_t m_bytes;
  size_t m_slot;
  long m_latencyMark;
  long m_retainMark;
  int m_wraps;

  std::shared_ptr<LineageGraph> m_graph;

 public:
  Task();
  Task(int taskId, const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch = nullptr, TaskType type = TaskType::PROCESS);
  void set(int taskId, const std::shared_ptr<WindowBatch>& batch, const std::shared_ptr<WindowBatch>& rBatch = nullptr, TaskType type = TaskType::PROCESS);
  int run(int pid);
  void outputWindowBatchResult(const std::shared_ptr<WindowBatch>& result);
  int getTaskId();
  int getQueryId();
  void setNumaNodeId(int node);
  int getNumaNodeId();
  void setLineageGraph(std::shared_ptr<LineageGraph> &graph);
  std::shared_ptr<LineageGraph> &getLineageGraph();
  void setInsertion(char *buffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps);
  void setInsertion(std::shared_ptr<UnboundedQueryBuffer> &buffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps);
  void setInsertion(void *rdmaBuffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps);
  ~Task();
};