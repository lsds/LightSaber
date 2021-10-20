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
      size_t batchSize, int taskId, long freePointer1,
      long freePointer2, Query *query, QueryBuffer *buffer,
      WindowDefinition *window, TupleSchema *schema, long latencyMark,
      long prevFreePointer1 = -1, long prevFreePointer2 = -1) {
    std::shared_ptr<WindowBatch> windowBatch;
    bool hasRemaining = pool.try_pop(windowBatch);
    if (!hasRemaining) {
      count.fetch_add(1, std::memory_order_seq_cst);
      windowBatch = std::make_shared<WindowBatch>(batchSize, taskId, freePointer1, freePointer2,
                                                  query, buffer, window, schema, latencyMark,
                                                  prevFreePointer1, prevFreePointer2);
    } else if (SystemConf::getInstance().LINEAGE_ON) {
      while (windowBatch.use_count() != 1) {
        hasRemaining = pool.try_pop(windowBatch);
        if (!hasRemaining) {
          count.fetch_add(1, std::memory_order_seq_cst);
          windowBatch = std::make_shared<WindowBatch>(batchSize, taskId, freePointer1, freePointer2,
                                                      query, buffer, window, schema, latencyMark,
                                                      prevFreePointer1, prevFreePointer2);
        }
      }
    }
    windowBatch->set(batchSize, taskId, freePointer1, freePointer2,
                     query, buffer, window, schema, latencyMark,
                     prevFreePointer1, prevFreePointer2);
    return windowBatch;
  }

  void free(const std::shared_ptr<WindowBatch>& windowBatch) {
    if (windowBatch->getLineageGraph()) {
      throw std::runtime_error("error: invalid place for a graph");
    }
    windowBatch->clear();
    pool.push(windowBatch);
  }

  long getCount() {
    return count.load();
  }
};


