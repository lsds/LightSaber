#include "utils/QueryApplication.h"
#include "utils/Query.h"
#include "monitors/PerformanceMonitor.h"
#include "tasks/Task.h"
#include "processor/TaskProcessorPool.h"
#include "dispatcher/TaskDispatcher.h"
#include "utils/Utils.h"

QueryApplication::QueryApplication(std::vector<std::shared_ptr<Query>> &queries)
    : m_numOfThreads(SystemConf::getInstance().WORKER_THREADS),
      m_queries(queries), m_numOfQueries((int) queries.size()), m_numberOfUpstreamQueries(0),
      m_taskQueueCapacity(2 * queries.size() *
          (SystemConf::getInstance().CIRCULAR_BUFFER_SIZE /
              SystemConf::getInstance().BATCH_SIZE)),
      m_queue(std::make_shared<TaskQueue>(m_taskQueueCapacity)),
      //m_queue(std::make_shared<TaskQueue>(2 * queries.size() * (SystemConf::getInstance().CIRCULAR_BUFFER_SIZE
      //    / SystemConf::getInstance().BATCH_SIZE))),
      m_workerPool(std::make_shared<TaskProcessorPool>(m_numOfThreads, m_queue)) {}

void QueryApplication::processData(std::vector<char> &values, long latencyMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatch(values.data(), values.size(), latencyMark);
  }
}

void QueryApplication::setup() {
  /* Bind main thread to CPU core 0 */
  Utils::bindProcess(0);
  m_workerPool->start();
  for (auto &q: m_queries) {
    q->setParent(this);
    if (q->isMostUpstream())
      setDispatcher(q->getTaskDispatcher());
  }
  m_performanceMonitor = std::make_unique<PerformanceMonitor>(*this);
  m_performanceMonitorThread = std::thread(std::ref(*m_performanceMonitor));
  m_performanceMonitorThread.detach();
}

void QueryApplication::setDispatcher(std::shared_ptr<TaskDispatcher> dispatcher) {
  m_numberOfUpstreamQueries++;
  m_dispatchers.push_back(dispatcher);
}

std::shared_ptr<TaskQueue> QueryApplication::getTaskQueue() {
  return m_queue;
}

int QueryApplication::getTaskQueueSize() {
  return (int) m_queue->size_approx();
}

size_t QueryApplication::getTaskQueueCapacity() { return m_taskQueueCapacity; }

std::vector<std::shared_ptr<Query>> QueryApplication::getQueries() {
  return m_queries;
}

std::shared_ptr<TaskProcessorPool> QueryApplication::getTaskProcessorPool() {
  return m_workerPool;
}

int QueryApplication::numberOfQueries() {
  return m_numOfQueries;
}

int QueryApplication::numberOfUpStreamQueries() {
  return m_numberOfUpstreamQueries;
}

