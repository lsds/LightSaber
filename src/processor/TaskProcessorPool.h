#pragma once

#include <vector>
#include <utils/Utils.h>

#include "processor/TaskProcessor.h"

/*
 * \brief This class creates a pool of worker threads.
 *
 * */

class TaskProcessorPool {
 private:
  int m_workers;
  std::weak_ptr<TaskQueue> m_queue;
  std::vector<std::thread> m_threads;
  std::vector<std::unique_ptr<TaskProcessor>> m_processors;

  /* Information used for pining worker threads to cores in-order based on the socket topology */
  std::vector<int> m_orderedCores;

 public:
  TaskProcessorPool(int workers, std::shared_ptr<TaskQueue> queue) :
      m_workers(workers), m_queue(queue), m_processors(workers) {

    std::cout << "[DBG] " + std::to_string(workers) + " threads" << std::endl;

    std::cout << "[DBG] CPU-only execution" << std::endl;
    for (int i = 0; i < workers; i++) {
      m_processors[i] = std::make_unique<TaskProcessor>(i, queue);
      /* Enable monitoring */
      //this.processors[i]->enableMonitoring();
    }
  }

  void start() {
    Utils::getOrderedCores(m_orderedCores);
    for (int i = 0; i < m_workers; i++) {
      m_threads.emplace_back(std::thread(*m_processors[i]));
      Utils::bindProcess(m_threads[i], m_orderedCores[i+1]);
    }
  }

  long getProcessedTasks(int pid, int qid) {
    return m_processors[pid]->getProcessedTasks(qid);
  }

  double mean(int pid) {
    return m_processors[pid]->mean();
  }

  double stdv(int pid) {
    return m_processors[pid]->stdv();
  }

  ~TaskProcessorPool() = default;
};