#include "tasks/Task.h"
#include "tasks/WindowBatch.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "result/ResultHandler.h"
#include "dispatcher/TaskDispatcher.h"

Task::Task() : m_batch(nullptr), m_taskId(-1) {}

Task::Task(int taskId, std::shared_ptr<WindowBatch> batch) :
    m_batch(batch), m_taskId(taskId), m_queryId(batch->getQuery()->getId()) {}

void Task::set(int taskId, std::shared_ptr<WindowBatch> batch) {
  m_taskId = taskId;
  m_batch = batch;
  m_queryId = batch->getQuery()->getId();
  m_numaNodeId = batch->getNumaNodeId();
}

int Task::run(int pid) {
  Query *query = m_batch->getQuery();
  QueryOperator *next = query->getMostUpstreamOperator();

  if (next->getDownstream() != nullptr)
    throw std::runtime_error("error: execution of chained query operators is not yet tested");

  next->getCode().processData(m_batch, *this, pid);

  if (m_batch == nullptr)
    return 0;

  auto handler = query->getResultHandler();
  handler->forwardAndFree(m_batch.get());

  WindowBatchFactory::getInstance().free(m_batch);
  return 0;
}

void Task::outputWindowBatchResult(std::shared_ptr<WindowBatch> result) {
  m_batch = result;
}

int Task::getTaskId() {
  return m_taskId;
}

int Task::getQueryId() {
  return m_queryId;
}

int Task::getNumaNodeId() {
  return m_numaNodeId;
}

Task::~Task() = default;
