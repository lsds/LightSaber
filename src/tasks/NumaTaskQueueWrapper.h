#pragma once

#include <vector>

#include "tbb/concurrent_queue.h"

#include "tasks/ConcurrentQueue.h"

class Task;

/*
 * \brief This class is a wrapper around multiple concurrent queues
 * that enable the NUMA-aware scheduling of tasks to workers based on data locality.
 *
 * */

class NumaTaskQueueWrapper {
 private:
  using TaskQueueUnit = moodycamel::ConcurrentQueue<std::shared_ptr<Task>>;

  const int m_cpusPerNode;
  const int m_numaNodes;
  const size_t m_capacity;
  const size_t m_queueCapacity;
  std::vector<std::unique_ptr<TaskQueueUnit>> m_queues;

  const bool m_workSteal = true;

 public:

  NumaTaskQueueWrapper(size_t capacity);

  bool try_enqueue(std::shared_ptr<Task> const &task);

  bool try_dequeue(std::shared_ptr<Task> &task, int numaNodeId);

  size_t size_approx() const;

  bool isReady();
};