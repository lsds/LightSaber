#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "tbb/concurrent_queue.h"

#include "utils/SystemConf.h"
#include "PartialWindowResults.h"

/*
 * \brief This class creates multiple pools of partial results, one for each available worker.
 *
 * At the moment, all the elements of the pools have a predefined size and this has
 * to change for more dynamic environments.
 *
 * */

class PartialWindowResultsFactory {
 private:
  const int m_numberOfThreads;
  std::atomic<long> m_count;
  /*static */std::vector<tbb::concurrent_queue<std::shared_ptr<PartialWindowResults>>> m_poolSeqMem;
  std::vector<tbb::concurrent_queue<std::shared_ptr<PartialWindowResults>>> m_poolPtrs;
  std::vector<tbb::concurrent_queue<std::shared_ptr<PartialWindowResults>>> m_poolSeqMemResults;
  PartialWindowResultsFactory() : m_numberOfThreads(SystemConf::getInstance().WORKER_THREADS),
                                  m_poolSeqMem(m_numberOfThreads),
                                  m_poolPtrs(m_numberOfThreads),
                                  m_poolSeqMemResults(m_numberOfThreads) {};

 public:

  static PartialWindowResultsFactory &getInstance() {
    static PartialWindowResultsFactory instance;
    return instance;
  }

  PartialWindowResultsFactory(PartialWindowResultsFactory const &) = delete;
  void operator=(PartialWindowResultsFactory const &) = delete;

  std::shared_ptr<PartialWindowResults> newInstance(int pid) {
    std::shared_ptr<PartialWindowResults> partialWindowResults;
    bool hasRemaining = m_poolSeqMem[pid].try_pop(partialWindowResults);
    if (!hasRemaining) {
      m_count.fetch_add(1);
      partialWindowResults =
          std::make_shared<PartialWindowResults>(pid, SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE);
    }
    partialWindowResults->init();

    /*if (partialWindowResults.use_count() != 1) {
      std::cout << "warning: the partial result has a reference counter of " + std::to_string(partialWindowResults.use_count()) << std::endl;
      //throw std::runtime_error("error: the partial result should have only one reference when we free it: " +
      //    std::to_string(partialWindowResults.use_count()));
    }*/

    return partialWindowResults;
  }

  std::shared_ptr<PartialWindowResults> newInstance(int pid, size_t capacity, int type = 1) {
    std::shared_ptr<PartialWindowResults> partialWindowResults;
    if (type == 1) {
      bool hasRemaining = m_poolPtrs[pid].try_pop(partialWindowResults);
      if (!hasRemaining) {
        m_count.fetch_add(1);
        partialWindowResults = std::make_shared<PartialWindowResults>(pid, capacity, 1);
      }
    } else if (type == 2) {
      bool hasRemaining = m_poolSeqMemResults[pid].try_pop(partialWindowResults);
      if (!hasRemaining) {
        m_count.fetch_add(1);
        partialWindowResults = std::make_shared<PartialWindowResults>(pid, capacity, 2);
      }
    } else {
      throw std::runtime_error("error: wrong type of PartialWindowResults");
    }
    partialWindowResults->init();
    return partialWindowResults;
  }

  void free(int pid, std::shared_ptr<PartialWindowResults> &partialWindowResults) {
    /*if (partialWindowResults.use_count() != 1) {
      std::cout << "warning: the partial result has a reference counter of " + std::to_string(partialWindowResults.use_count()) << std::endl;
      throw std::runtime_error("error: the partial result should have only one reference when we free it: " +
          std::to_string(partialWindowResults.use_count()));
    }*/
    if (pid >= m_numberOfThreads)
      throw std::runtime_error("error: attempting to free partial window with pid: " + std::to_string(pid)
                               + " >= " + std::to_string(m_numberOfThreads));

    if (partialWindowResults->getType() == 0) {
      m_poolSeqMem[pid].push(partialWindowResults);
    } else if (partialWindowResults->getType() == 1) {
      m_poolPtrs[pid].push(partialWindowResults);
    } else {
      m_poolSeqMemResults[pid].push(partialWindowResults);
    }
  }

  long getCount() {
    return m_count.load();
  }
};
