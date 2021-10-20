#pragma once

#include <atomic>
#include <memory>

#include "tbb/concurrent_queue.h"

#include "UnboundedQueryBuffer.h"
#include "utils/SystemConf.h"

/*
 * \brief This class creates a single pool of buffers used by all workers to perform stateless operations.
 *
 * */

class UnboundedQueryBufferFactory {
 private:
  const int m_numberOfThreads;
  std::atomic<long> count;
  tbb::concurrent_queue<std::shared_ptr<UnboundedQueryBuffer>> m_pool;
  std::vector<tbb::concurrent_queue<std::shared_ptr<UnboundedQueryBuffer>>> m_poolCB, m_poolNB;
  UnboundedQueryBufferFactory() : m_numberOfThreads(SystemConf::getInstance().WORKER_THREADS), m_poolCB(m_numberOfThreads),
        m_poolNB(m_numberOfThreads + 1){};

 public:
  static UnboundedQueryBufferFactory &getInstance() {
    static UnboundedQueryBufferFactory instance;
    return instance;
  }

  UnboundedQueryBufferFactory(UnboundedQueryBufferFactory const &) = delete;
  void operator=(UnboundedQueryBufferFactory const &) = delete;

  std::shared_ptr<UnboundedQueryBuffer> newInstance() {
    std::shared_ptr<UnboundedQueryBuffer> buffer;
    bool hasRemaining = m_pool.try_pop(buffer);
    if (!hasRemaining) {
      int id = (int) count.fetch_add(1);
      buffer = std::make_shared<UnboundedQueryBuffer>(UnboundedQueryBuffer(id,
                                                                           SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE));
    }
    return buffer;
  }

  void free(std::shared_ptr<UnboundedQueryBuffer> &buffer) {
    // buffer->clear();
    buffer->setPosition(0);
    m_pool.push(buffer);
  }

  std::shared_ptr<UnboundedQueryBuffer> newInstance(int pid) {
    if (pid >= m_numberOfThreads)
      throw std::runtime_error("error: invalid pid for creating an unbounded buffer");
    std::shared_ptr<UnboundedQueryBuffer> buffer;
    bool hasRemaining = m_poolCB[pid].try_pop(buffer);
    if (!hasRemaining) {
      count.fetch_add(1);
      buffer = std::make_shared<UnboundedQueryBuffer>(UnboundedQueryBuffer(pid,
                                                                           SystemConf::getInstance().BLOCK_SIZE));
    }
    return buffer;
  }

  void free(int pid, std::shared_ptr<UnboundedQueryBuffer> &buffer) {
    // buffer->clear();
    buffer->setPosition(0);
    m_poolCB[pid].push(buffer);
  }

  std::shared_ptr<UnboundedQueryBuffer> newNBInstance(int pid) {
    if (pid >= m_numberOfThreads + 1)
      throw std::runtime_error("error: invalid pid for creating an unbounded buffer");
    std::shared_ptr<UnboundedQueryBuffer> buffer;
    bool hasRemaining = m_poolNB[pid].try_pop(buffer);
    if (!hasRemaining) {
      buffer = std::make_shared<UnboundedQueryBuffer>(UnboundedQueryBuffer(pid,
                                                                           SystemConf::getInstance().BATCH_SIZE));
    }
    return buffer;
  }

  void freeNB(int pid, std::shared_ptr<UnboundedQueryBuffer> &buffer) {
    //buffer->clear();
    buffer->setPosition(0);
    m_poolNB[pid].push(buffer);
  }

  long getCount() {
    return count.load();
  }
};
