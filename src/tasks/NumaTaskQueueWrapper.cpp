#include <cmath>

#include "NumaTaskQueueWrapper.h"
#include "utils/SystemConf.h"
#include "utils/Utils.h"
#include "Task.h"

NumaTaskQueueWrapper::NumaTaskQueueWrapper(size_t capacity) :
#if defined(HAVE_NUMA)
    m_cpusPerNode(Utils::getNumberOfCoresPerSocket()),
    m_numaNodes(
        (numa_available() < 0) ? 1 :
        (int) std::ceil(((double) SystemConf::getInstance().WORKER_THREADS + 1) / m_cpusPerNode)),
#else
m_cpusPerNode(SystemConf::getInstance().THREADS),
m_numaNodes(1),
#endif
    m_capacity(capacity), m_queueCapacity(m_capacity / m_numaNodes),
    m_queues((size_t) m_numaNodes) {
  for (int i = 0; i < m_numaNodes; ++i) {
    m_queues[i] = std::make_unique<TaskQueueUnit>(m_queueCapacity);
  }
}

bool NumaTaskQueueWrapper::try_enqueue(std::shared_ptr<Task> const &task) {
  /*std::cout << "[DBG] Adding in TaskQueue: " << task->getQueryId() << " queryid "
  << task->getNumaNodeId() << " numaId " << task->getTaskId() << " taskId " << std::endl;*/
  return m_queues[task->getNumaNodeId()]->try_enqueue(task);
}

bool NumaTaskQueueWrapper::try_dequeue(std::shared_ptr<Task> &task, int numaNodeId) {

  // first try to find a task in the local numa node
  for (int i = 0; i < 3; ++i) {
    m_queues[numaNodeId]->try_dequeue(task);
    if (task == nullptr) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(1));
    } else {
      return true;
    }
  }

  if (m_workSteal) {
    // then try to steal from the next numa node
    if (task == nullptr) {
      numaNodeId = (numaNodeId + 1) % m_numaNodes;
      m_queues[numaNodeId]->try_dequeue(task);
    }
  }

  bool isFound = false;
  if (task != nullptr) {
    isFound = true;
  }

  return isFound;
}

size_t NumaTaskQueueWrapper::size_approx() const {
  size_t size = 0;
  for (auto &q : m_queues) {
    size += q->size_approx();
  }
  return size;
}

bool NumaTaskQueueWrapper::isReady() {
  for (int i = 0; i < m_numaNodes; ++i) {
    if (m_queues[i] == nullptr)
      return false;
  }
  return true;
}