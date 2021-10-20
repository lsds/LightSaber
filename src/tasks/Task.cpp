#include "tasks/Task.h"

#include "buffers/UnboundedQueryBuffer.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "result/ResultHandler.h"
#include "tasks/WindowBatch.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Query.h"
#include "utils/QueryApplication.h"
#include "utils/QueryOperator.h"

Task::Task() : m_leftBatch(nullptr), m_rightBatch(nullptr), m_taskId(-1) {}

Task::Task(int taskId, const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch, TaskType type) :
    m_leftBatch(lBatch), m_rightBatch(rBatch), m_taskId(taskId), m_queryId(lBatch->getQuery()->getId()), m_type(type) {}

void Task::set(int taskId, const std::shared_ptr<WindowBatch>& lBatch, const std::shared_ptr<WindowBatch>& rBatch, TaskType type) {
  m_taskId = taskId;
  m_leftBatch = lBatch;
  m_rightBatch = rBatch;
  if (!m_leftBatch)
    throw std::runtime_error("error: the left batch of the task has to be set");
  m_queryId = lBatch->getQuery()->getId();
  m_numaNodeId = lBatch->getNumaNodeId(); // for now pick numa locality based on the first batch only
  m_type = type;
}

int Task::run(int pid) {
  Query *query = m_leftBatch->getQuery();
  QueryOperator *next = query->getMostUpstreamOperator();

  if (next->getDownstream() != nullptr)
    throw std::runtime_error("error: execution of chained query operators is not yet tested");

  m_leftBatch->setPid(pid);
  //std::cout << "[DBG] running a "<< taskTypeToString(m_type) << " task with id " << m_taskId << std::endl;
  if (m_type == TaskType::RECOVER) {
    query->getParent()->getCheckpointCoordinator()->recover(pid, query->getId());
    WindowBatchFactory::getInstance().free(m_leftBatch);
    return 0;
  }
  if (m_type == TaskType::INSERT) {
    auto buffer = query->getBuffer();
#if defined(TCP_INPUT)
    buffer->putRows(pid, m_qBuffer, m_bytes, m_slot, m_latencyMark, m_retainMark, m_wraps);
#elif defined(RDMA_INPUT)
    buffer->putRows(pid, m_rdmaBuffer, m_bytes, m_slot, m_latencyMark, m_retainMark, m_wraps);
#else
    buffer->putRows(pid, m_buffer, m_bytes, m_slot, m_latencyMark, m_retainMark, m_wraps);
#endif
    WindowBatchFactory::getInstance().free(m_leftBatch);
    return 0;
  }
  if (m_type == TaskType::PROCESS || m_type == TaskType::ONLY_PROCESS) {
    //std::cout << "[DBG] running a "<< taskTypeToString(m_type) << " task with id " << m_taskId << std::endl;
    // update here timestamps in the case that data is replayed from memory
    if (m_leftBatch->hasTimestampOffset())
      m_leftBatch->updateTimestamps();

    if (query->isTaskDropped(m_taskId)) {
      std::cout << "[DBG] dropping "<< taskTypeToString(m_type) << " task with id " << m_taskId << std::endl;
      if (auto &lGraph = m_leftBatch->getLineageGraph()) {
        lGraph->freePersistentState(m_queryId);
        if (lGraph.use_count() == 1)
          LineageGraphFactory::getInstance().free(lGraph);
        lGraph.reset();
      }
      if (auto &rGraph = m_rightBatch->getLineageGraph()) {
        rGraph->freePersistentState(m_queryId);
        if (rGraph.use_count() == 1)
          LineageGraphFactory::getInstance().free(rGraph);
        rGraph.reset();
      }
      WindowBatchFactory::getInstance().free(m_leftBatch);
      if (m_rightBatch) {
        WindowBatchFactory::getInstance().free(m_rightBatch);
      }
      return 0;
    }
    if (!m_rightBatch) {
      next->getCode().processData(m_leftBatch, *this, pid);
    } else {
      next->getCode().processData(m_leftBatch, m_rightBatch, *this, pid);
      // Operator `next` calls `outputWindowBatchResult()` and updates `batch1`; `batch2`, if not null, is no longer needed
      WindowBatchFactory::getInstance().free(m_rightBatch);
    }

    if (m_leftBatch == nullptr) return 0;
  }

  if (m_type == TaskType::CHECKPOINT) {
    query->getParent()->getCheckpointCoordinator()->checkpoint(pid, query->getId());
  }

  auto handler = query->getResultHandler();
  handler->forwardAndFree(m_leftBatch.get());

  WindowBatchFactory::getInstance().free(m_leftBatch);
  return 0;
}

void Task::outputWindowBatchResult(const std::shared_ptr<WindowBatch>& result) {
  m_leftBatch = result;
}

int Task::getTaskId() {
  return m_taskId;
}

int Task::getQueryId() {
  return m_queryId;
}

void Task::setNumaNodeId(int node) {
  m_numaNodeId = node;
}

int Task::getNumaNodeId() {
  return m_numaNodeId;
}

void Task::setLineageGraph(std::shared_ptr<LineageGraph> &graph) {
  /*if (m_graph && m_graph.use_count() == 1) {
    LineageGraphFactory::getInstance().free(m_graph);
  }*/
  m_graph = graph;
}

std::shared_ptr<LineageGraph> &Task::getLineageGraph() {
  return m_graph;
}

void Task::setInsertion(char *buffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps) {
  m_buffer = buffer;
  m_bytes = bytes;
  m_slot = slot;
  m_latencyMark = latencyMark;
  m_retainMark = retainMark;
  m_wraps = wraps;
}

void Task::setInsertion(std::shared_ptr<UnboundedQueryBuffer> &buffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps) {
  m_buffer = buffer->getBuffer().data();
  m_qBuffer = buffer;
  m_bytes = bytes;
  m_slot = slot;
  m_latencyMark = latencyMark;
  m_retainMark = retainMark;
  m_wraps = wraps;
}

void Task::setInsertion(void *rdmaBuffer, size_t bytes, size_t slot, long latencyMark, long retainMark, int wraps) {
  m_rdmaBuffer = rdmaBuffer;
  m_bytes = bytes;
  m_slot = slot;
  m_latencyMark = latencyMark;
  m_retainMark = retainMark;
  m_wraps = wraps;
}

Task::~Task() = default;
