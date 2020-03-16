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
  std::atomic<long> count;
  tbb::concurrent_queue<std::shared_ptr<UnboundedQueryBuffer>> pool;
  UnboundedQueryBufferFactory() {};

 public:
  static UnboundedQueryBufferFactory &getInstance() {
    static UnboundedQueryBufferFactory instance;
    return instance;
  }

  UnboundedQueryBufferFactory(UnboundedQueryBufferFactory const &) = delete;
  void operator=(UnboundedQueryBufferFactory const &) = delete;

  std::shared_ptr<UnboundedQueryBuffer> newInstance() {
    std::shared_ptr<UnboundedQueryBuffer> buffer;
    bool hasRemaining = pool.try_pop(buffer);
    if (!hasRemaining) {
      int id = (int) count.fetch_add(1);
      buffer = std::make_shared<UnboundedQueryBuffer>(UnboundedQueryBuffer(id,
                                                                           SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE));
    }
    return buffer;
  }

  void free(std::shared_ptr<UnboundedQueryBuffer> &buffer) {
    buffer->clear();
    pool.push(buffer);
  }

  long getCount() {
    return count.load();
  }
};
