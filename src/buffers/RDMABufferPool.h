#pragma once

#include <atomic>
#include <memory>

#include "RDMA/infinity/infinity.h"

/*
 * \brief This class creates a single pool of buffers used by all workers to perform RDMA operations.
 *
 * */

class RDMABufferPool {
 private:
  std::atomic<long> count{};
  infinity::core::Context *m_context = nullptr;
  RDMABufferPool() = default;

 public:
  static RDMABufferPool &getInstance() {
    static RDMABufferPool instance;
    return instance;
  }

  RDMABufferPool(RDMABufferPool const &) = delete;
  void operator=(RDMABufferPool const &) = delete;

  void setContext(infinity::core::Context *context) {
    if (!context) {
      throw std::runtime_error("error: setup the context");
    }
    m_context = context;
  }

  void free(infinity::core::receive_element_t *elem) {
    if (!m_context) {
      throw std::runtime_error("error: setup the context");
    }
    m_context->postReceiveBuffer(elem->buffer);
    delete(elem);
  }

  void free(infinity::memory::Buffer *elem) {
    if (!m_context) {
      throw std::runtime_error("error: setup the context");
    }
    m_context->postReceiveBuffer(elem);
    delete(elem);
  }

  long getCount() {
    return count.load();
  }
};
