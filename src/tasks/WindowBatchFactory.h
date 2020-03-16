#pragma once

#include <atomic>
#include <memory>

#include "tbb/concurrent_queue.h"

#include "tasks/WindowBatch.h"
#include "utils/Query.h"

/*
 * This is a pool of @WindowBatches used by the @TaskDispatcher.
 *
 * */

class WindowBatchFactory {
 private:
  std::atomic<long> count;
  tbb::concurrent_queue<std::shared_ptr<WindowBatch>> pool;
  WindowBatchFactory() {};

 public:
  static WindowBatchFactory &getInstance() {
    static WindowBatchFactory instance;
    return instance;
  }

  WindowBatchFactory(WindowBatchFactory const &) = delete;
  void operator=(WindowBatchFactory const &) = delete;

  std::shared_ptr<WindowBatch> newInstance(
      size_t batchSize, int taskId, int freePointer,
      Query *query, QueryBuffer *buffer,
      WindowDefinition *window, TupleSchema *schema, long latencyMark) {
    std::shared_ptr<WindowBatch> windowBatch;
    bool hasRemaining = pool.try_pop(windowBatch);
    if (!hasRemaining) {
      count.fetch_add(1, std::memory_order_seq_cst);
      windowBatch = std::make_shared<WindowBatch>(batchSize, taskId, freePointer,
                                                  query, buffer, window, schema, latencyMark);
    }
    windowBatch->set(batchSize, taskId, freePointer,
                     query, buffer, window, schema, latencyMark);
    return windowBatch;
  }

  void free(std::shared_ptr<WindowBatch> &windowBatch) {
    windowBatch->clear();
    pool.push(windowBatch);
  }

  long getCount() {
    return count.load();
  }
};


