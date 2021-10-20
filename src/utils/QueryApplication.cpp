#include "utils/QueryApplication.h"

#include <unordered_set>

#include "buffers/QueryBuffer.h"
#include "checkpoint/BlockManager.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "checkpoint/LineageGraphFactory.h"
#include "dispatcher/ITaskDispatcher.h"
#include "dispatcher/TaskDispatcher.h"
#include "filesystem/File.h"
#include "filesystem/FileSystemDisk.h"
#include "monitors/CompressionMonitor.h"
#include "monitors/PerformanceMonitor.h"
#include "processor/TaskProcessorPool.h"
#include "result/ResultHandler.h"
#include "tasks/Task.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/Utils.h"

QueryApplication::QueryApplication(std::vector<std::shared_ptr<Query>> &queries,
                                   bool checkpointEnabled, bool clearFiles)
    : m_numOfThreads(SystemConf::getInstance().WORKER_THREADS),
      m_queries(queries),
      m_numOfQueries((int)queries.size()),
      m_numberOfUpstreamQueries(0),
      m_taskQueueCapacity(8 * queries.size() *
          (SystemConf::getInstance().CIRCULAR_BUFFER_SIZE /
              SystemConf::getInstance().BATCH_SIZE)),
      m_queue(std::make_shared<TaskQueue>(m_taskQueueCapacity)),
      m_workerPool(std::make_shared<TaskProcessorPool>(m_numOfThreads, m_queue)),
      m_checkpointEnabled(checkpointEnabled),
      m_rates(m_numOfQueries, 1), m_clearFiles(clearFiles) {

  if (SystemConf::getInstance().LINEAGE_ON) {
    LineageGraphFactory::getInstance().setGraph(m_queries);
    m_fileStore = std::make_shared<BlockManager>(m_queries, m_clearFiles);
  }
}

void QueryApplication::processData(std::vector<char> &values,
                                   long latencyMark, long retainMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatch(values.data(), values.size() / m_rates[i], latencyMark, retainMark);
  }

  /*if (m_dispatchers.size() > 1) {
    for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
      m_dispatchers[i]->tryToConsume();
    }
  }*/
}

void QueryApplication::processData(std::shared_ptr<UnboundedQueryBuffer> &values,
                                   long latencyMark, long retainMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatch(values, latencyMark, retainMark);
  }
}

void QueryApplication::processData(void *values, int length, long latencyMark, long retainMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatch(values, length, latencyMark, retainMark);
  }
}

void QueryApplication::processFirstStream(std::vector<char> &values, long latencyMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatchToFirstStream(values.data(), values.size(), latencyMark);
  }
}

void QueryApplication::processSecondStream(std::vector<char> &values, long latencyMark) {
  for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
    m_dispatchers[i]->dispatchToSecondStream(values.data(), values.size(), latencyMark);
  }
}

void QueryApplication::recoverData() {
  auto t1 = std::chrono::high_resolution_clock::now();

  // wait until all the tasks for checkpoint recovery have been created
  if (SystemConf::getInstance().CHECKPOINT_ON) {
    while (!m_clearFiles)
      ;
  }

  std::cout << "[DBG] starting to recover the data for the input buffers" << std::endl;

  //perform a reversal topological order traversal for restoring offsets
  std::vector<std::shared_ptr<Query>> sortedQueries;
  std::vector<bool> visited (m_numOfQueries, false);
  std::stack<int> stack;
  for (int i = 0; i < m_numOfQueries; i++) {
    if (visited[i] == false) {
      topologicalSort(i, visited, stack);
    }
  }
  while (!stack.empty()) {
    sortedQueries.push_back(m_queries[stack.top()]);
    stack.pop();
  }

  size_t maxIterations = 0;
  for (unsigned long idx =  0; idx < sortedQueries.size(); idx++) {
    auto query = sortedQueries[idx];
    //auto buffer = m_dispatchers[i]->getBuffer();
    auto buffer = query->getTaskDispatcher()->getBuffer();

    auto temp = buffer->getUnsafeRemainingBytes()/buffer->getBatchSize();
    maxIterations = std::max(temp, maxIterations);
    buffer->setNumberOfSlotsToRecover(temp);
    int lastTask = 0;
    if (m_dispatchers.size() == 1 || query->isMostDownstream()) {
      // todo: the most downstream operators would have to read this from an external sink
      // todo: generalize this for pipelined queries
      lastTask = (int)(buffer->getUnsafeStartPointer()/buffer->getBatchSize()) + 1;
    } else {
      // todo: fix joins
      throw std::runtime_error("error: recovering joins is not supported yet");
      auto startPtr = query->getDownstreamQuery()->getOperator()->getInputPtr(true);
      lastTask = (int)(startPtr/buffer->getBatchSize()) + 1;
    }
    std::cout << "[DBG] restarting from task " << lastTask << " for buffer "
              << buffer->getBufferId() << " with " << temp
              << " slots " << std::endl;
    query->getTaskDispatcher()->setTaskNumber(lastTask);

    long step; long offset;
    // todo: implement the following function to non p-stream buffers
    buffer->getStepAndOffset(step, offset);
    query->getTaskDispatcher()->setStepAndOffset(step, offset);
    buffer->prepareRecovery();
  }

  // parallel recovery of p-streams
  if (m_dispatchers.size() == 1) {
    auto buffer = m_dispatchers[0]->getBuffer();
    if (buffer->isPersistent()) {
      m_dispatchers[0]->recover();
    }
  } else {
    std::vector<std::thread> threads (m_dispatchers.size());
    for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
      auto buffer = m_dispatchers[i]->getBuffer();
      if (buffer->isPersistent()) {
        threads[i] = std::thread([&]{
          m_dispatchers[i]->recover();
        });
      } else {
        threads[i] = std::thread([&]{});
      }
    }
    for (unsigned long i = 0; i < m_dispatchers.size(); ++i) {
     threads[i].join();
    }
  }

  if (m_checkpointCoordinator)
    m_checkpointCoordinator->setReady();

  auto t2 = std::chrono::high_resolution_clock::now();
  auto time_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  std::cout << "[DBG] recovery duration " << time_span.count() << std::endl;
}

bool QueryApplication::tryProcessData(std::vector<char> &values,
                                   long latencyMark) {
  throw std::runtime_error("error: this function is not supported yet");
  if (m_dispatchers.size() > 1)
    throw std::runtime_error("error: unsupported number of dispatchers");
  return m_dispatchers[0]->tryDispatchOrCreateTask(values.data(), values.size(), latencyMark);
}

void QueryApplication::setup() {
  /* Bind main thread to CPU core 0 */
  Utils::bindProcess(0);
  m_workerPool->start();
  std::unordered_set<int> qIds;
  for (auto &q : m_queries) {
    if(!qIds.insert(q->getId()).second) {
      throw std::runtime_error("error: duplicate query ids");
    }
    q->setParent(this);
    if (q->isMostUpstream()) setDispatcher(q->getTaskDispatcher());
    if (q->isMostDownstream()) {
#if defined(TCP_OUTPUT)
      q->getResultHandler()->setupSocket();
#elif defined(RDMA_OUTPUT)
      q->getResultHandler()->setupSocket();
#endif
    }
  }
  m_performanceMonitor = std::make_unique<PerformanceMonitor>(*this);
  m_performanceMonitorThread = std::thread(std::ref(*m_performanceMonitor));
  m_performanceMonitorThread.detach();

  if (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON) {
    m_compressionMonitor = std::make_unique<CompressionMonitor>(this);
    m_compressionMonitorThread = std::thread(std::ref(*m_compressionMonitor));
    m_compressionMonitorThread.detach();
  }

  /* Setup the checkpoint coordinator */
  if (m_checkpointEnabled) {
    m_filesystem = std::make_shared<disk_t>(SystemConf::FILE_ROOT_PATH);
    m_checkpointCoordinator = std::make_unique<FileBackedCheckpointCoordinator>(0, m_queries, &m_clearFiles, m_filesystem, true);
    m_checkpointCoordinatorThread = std::thread(std::ref(*m_checkpointCoordinator));
    m_checkpointCoordinatorThread.detach();
  }
}

void QueryApplication::setupRates(std::vector<int> &rates) {
  size_t i = 0;
  for (auto &r: m_rates) {
    r = rates[i++];
  }
}

void QueryApplication::setDispatcher(
    std::shared_ptr<ITaskDispatcher> dispatcher) {
  m_numberOfUpstreamQueries++;
  m_dispatchers.push_back(dispatcher);
}

std::shared_ptr<TaskQueue> QueryApplication::getTaskQueue() { return m_queue; }

size_t QueryApplication::getTaskQueueSize() { return m_queue->size_approx(); }

size_t QueryApplication::getTaskQueueCapacity() { return m_taskQueueCapacity; }

std::vector<std::shared_ptr<Query>> QueryApplication::getQueries() {
  return m_queries;
}

std::shared_ptr<TaskProcessorPool> QueryApplication::getTaskProcessorPool() {
  return m_workerPool;
}

FileBackedCheckpointCoordinator *QueryApplication::getCheckpointCoordinator() {
  return m_checkpointCoordinator.get();
}

long QueryApplication::getTimestampReference() {
  long timestamp = LONG_MAX;
  for (auto &q: m_queries) {
    timestamp = std::min(timestamp, q->getTimestampReference());
  }
  return timestamp;
}

long QueryApplication::getLastTimestamp() {
  long timestamp = LONG_MAX;
  for (auto &q: m_queries) {
    timestamp = std::min(timestamp, q->getLastTimestamp());
  }
  return timestamp;
}

int QueryApplication::numberOfQueries() { return m_numOfQueries; }

int QueryApplication::numberOfUpstreamQueries() {
  return m_numberOfUpstreamQueries;
}

QueryApplication::~QueryApplication() {
  for (auto &q : m_queries) {
    q->getBuffer()->~QueryBuffer();
    q->getSecondBuffer()->~QueryBuffer();
  }
  if (m_checkpointCoordinator) {
    m_checkpointCoordinator->~FileBackedCheckpointCoordinator();
  }
}

void QueryApplication::topologicalSort(int q, std::vector<bool> &visited, std::stack<int> &stack) {
  visited[q] = true;
  for (int i = 0; i < m_queries[q]->getNumberOfUpstreamQueries(); i++) {
    auto qId = m_queries[q]->getUpstreamQuery(i)->getId();
    if (!visited[qId]) {
      topologicalSort(qId, visited, stack);
    }
  }
  stack.push(q);
}