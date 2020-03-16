#pragma once

#include <vector>
#include <cstring>
#include <stdexcept>
#include <algorithm>
#include <mm_malloc.h>

#include "utils/SystemConf.h"

/*
 * \brief This class implements a buffer for partial results (or window fragments -- also called Shases).
 *
 * Depending on how the m_type variable is set, the partial windows are going to use a buffer
 * of elements or a buffer of pointer to elements. In the case of grouped aggregation, the pointer
 * implementation is preferred to avoid unwanted copies.
 * There are four types of partial results: open, closing, pending, and complete windows.
 *
 * */

class PartialWindowResults {
 private:
  size_t m_capacity;
  size_t m_position;
  ByteBuffer m_buffer;        /* The buffer that holds the partial window results */
  ByteBufferPtr m_bufferPtrs;
  std::vector<int> m_startPointers;
  const size_t m_partialWindows = (size_t) SystemConf::getInstance().PARTIAL_WINDOWS;
  int m_pid;                  /* The worker thread that requested this object */
  int m_count;
  int m_type;                 /* TYPE = 0 for sequential memory, TYPE = 1 for ptrs to memory */

 public:
  PartialWindowResults(int pid, size_t capacity, int type = 0) : m_capacity(capacity),
                                                                 m_position(0L),
                                                                 m_buffer((type != 1) ? capacity : 0),
                                                                 m_bufferPtrs((type == 1)
                                                                              ? (size_t) SystemConf::getInstance().PARTIAL_WINDOWS
                                                                              : 0),
                                                                 m_startPointers((type != 2)
                                                                                 ? SystemConf::getInstance().PARTIAL_WINDOWS
                                                                                 : 2),
                                                                 m_pid(pid),
                                                                 m_count(0),
                                                                 m_type(type) {
    //int node;
    //SystemConf::getInstance().findMemoryNodeForCPU(node);
    //std::cout << "[DBG] Thread " << sched_getcpu()  << " in " << node << " numa node." << std::endl;
    if (type == 1) {
      for (size_t i = 0; i < m_bufferPtrs.size(); ++i) {
        m_bufferPtrs[i] = (char *) _mm_malloc(capacity * sizeof(char), 64);
      }
    }
    //SystemConf::getInstance().findMemoryNodeForAddress(&bufferPtrs[2], node);
    //std::cout << "[DBG] ByteBuffer from " << sched_getcpu()  << " is allocated in " << node << " numa node." << std::endl;
  }

  int getThreadId() {
    return m_pid;
  }

  ByteBuffer &getBuffer() {
    if (m_type == 0)
      return m_buffer;
    else
      throw std::runtime_error("error: getting a byte buffer from partial window buffer of this type is not implemented");
  }

  ByteBufferPtr &getBufferPtrs() {
    if (m_type == 1)
      return m_bufferPtrs;
    else
      throw std::runtime_error(
          "error: getting a buffer of pointers from partial window buffer of this type is not implemented");
  }

  size_t getCapacity() {
    return m_capacity;
  }

  bool isEmpty() {
    return (m_count == 0);
  }

  void clear() {
    //std::fill(buffer.begin(), buffer.end(), 0);
  }

  void reset() {
    if (m_type == 0) {
      std::fill(m_startPointers.begin(), m_startPointers.end(), -1);
    } else if (m_type == 2) {
      m_startPointers[0] = -1;
      m_startPointers[1] = -1;
    }
    m_count = 0;
  }

  void init() {
    m_count = 0;
  }

  void nullify() {
    //buffer.clear();
    //for (int i = 0; i < partialWindows; ++i)
    //    startPointers[i] = -1;
    m_count = 0;
  }

  void increment() {
    if (m_count > (int) m_partialWindows)
      throw std::out_of_range("error: partial window result index out of bounds while incrementing the counter");
    m_startPointers[m_count] = (int) getPosition();
    m_count++;
  }

  void incrementCount(int cnt) {
    m_count += cnt;
    if (m_count > (int) m_partialWindows)
      throw std::out_of_range(
          "error: partial window result index out of bounds while incrementing the counter with a value");
  }

  void setCount(int cnt) {
    m_count = cnt;
    if (m_count > (int) m_partialWindows)
      throw std::out_of_range(
          "error: partial window result index out of bounds while incrementing the counter with a value");
  }

  int getStartPointer(int idx) {
    if (idx < 0 || idx > m_count)
      throw std::out_of_range("error: partial window result index out of bounds while trying to get a starting pointer");
    return m_startPointers[idx];
  }

  std::vector<int> &getStartPointers() {
    return m_startPointers;
  }

  int numberOfWindows() {
    return m_count;
  }

  int getType() {
    return m_type;
  }

  void append(ByteBuffer &windowBuffer, size_t length) {
    if (m_type == 0) {
      if (m_count >= (int) m_partialWindows)
        throw std::out_of_range("error: partial window result index out of bounds in append");
      m_startPointers[m_count++] = (int) getPosition();
      std::memcpy(m_buffer.data() + getPosition(), &(windowBuffer[0]), (length) * sizeof(char));
    } else {
      throw std::runtime_error("error: appending windows to a partial window buffer of this type is not implemented");
    }
  }

  void append(PartialWindowResults *closingWindows) {
    if (m_type == 0) {
      int offset = (int) getPosition();
      for (int wid = 0; wid < closingWindows->numberOfWindows(); ++wid) {
        if (m_count >= (int) m_partialWindows)
          throw std::out_of_range(
              "error: partial window result index out of bounds in append with closingWindows");
        m_startPointers[m_count++] = offset + closingWindows->getStartPointer(wid);
      }
      std::memcpy(m_buffer.data() + getPosition(), &(closingWindows->getBuffer()[0]),
                  (closingWindows->getPosition()) * sizeof(char));
    } else {
      throw std::runtime_error("error: appending windows to a partial window buffer of this type is not implemented");
    }
  }

  void prepend(PartialWindowResults *openingWindows, int startP, int added, long windowSize, int tupleSize) {
    if (m_type == 0) {

      int count_ = m_count + added;
      if (count_ >= (int) m_partialWindows)
        throw std::out_of_range("error: partial window result index out of bounds in prepend");
      if (count_ * windowSize * tupleSize > (long) m_buffer.size())
        throw std::out_of_range("error: partial window buffer size can't hold this amount of data");

      /* Shift-down windows */
      int norm = openingWindows->getStartPointer(startP);
      int endP = startP + added - 1;
      int offset = (int) (openingWindows->getStartPointer(endP) - norm + windowSize);
      for (int i = m_count - 1; i >= 0; i--) {
        m_startPointers[i + added] = m_startPointers[i] + offset;
        int src = m_startPointers[i] * tupleSize;
        int dst = m_startPointers[i + added] * tupleSize;
        setPosition(dst);
        std::memcpy(m_buffer.data() + getPosition(), &(m_buffer[src]),
                    (windowSize * tupleSize) * sizeof(char));
      }
      for (int i = 0, w = startP; i < added; ++i, ++w) {
        m_startPointers[i] = openingWindows->getStartPointer(w) - norm;
        int src = openingWindows->getStartPointer(w) * tupleSize;
        int dst = m_startPointers[i] * tupleSize;
        setPosition(dst);
        std::memcpy(m_buffer.data() + getPosition(), &(openingWindows->getBuffer()[src]),
                    (windowSize * tupleSize) * sizeof(char));
      }
      m_count = count_;
      setPosition(m_count * windowSize);
      m_startPointers[m_count] = (int) (m_count * windowSize);
    } else {

      int count_ = m_count + added;
      if (count_ >= (int) m_partialWindows)
        throw std::out_of_range("error: partial window result index out of bounds in prepend");
      if (count_ * windowSize * tupleSize > (long) (m_partialWindows * m_capacity))
        throw std::out_of_range("error: partial window buffer size can't hold this amount of data");

      /* Shift-down windows */
      int norm = openingWindows->getStartPointer(startP);
      int endP = startP + added - 1;
      int offset = (int) (openingWindows->getStartPointer(endP) - norm + windowSize);
      for (int i = m_count - 1; i >= 0; i--) {
        m_startPointers[i + added] = m_startPointers[i] + offset;
        //int src = startPointers[i] * tupleSize;
        int dst = m_startPointers[i + added] * tupleSize;
        setPosition(dst);
        // swap positions
        auto tempPtr = m_bufferPtrs[i];
        m_bufferPtrs[i] = m_bufferPtrs[i + added];
        m_bufferPtrs[i + added] = tempPtr;
      }
      for (int i = 0, w = startP; i < added; ++i, ++w) {
        m_startPointers[i] = openingWindows->getStartPointer(w) - norm;
        //int src = openingWindows->getStartPointer(w) * tupleSize;
        int dst = m_startPointers[i] * tupleSize;
        setPosition(dst);
        // swap positions
        auto tempPtr = openingWindows->getBufferPtrs()[i];
        openingWindows->getBufferPtrs()[i] = m_bufferPtrs[i];
        m_bufferPtrs[i] = tempPtr;
      }
      m_count = count_;
      setPosition(m_count * windowSize);
      m_startPointers[m_count] = (int) (m_count * windowSize);
    }
  }

  void setPosition(size_t pos) {
    if (m_type == 0) {
      if (pos > m_buffer.size())
        throw std::out_of_range("error: partial window buffer size can't hold this amount of data");
    } else {
      if (pos > m_partialWindows * m_capacity)
        throw std::out_of_range("error: partial window buffer size can't hold this amount of data");
    }
    m_position = pos;
  }

  size_t getPosition() {
    return m_position;
  }

  long getLong(size_t index) {
    if (m_type == 0) {
      auto p = (long *) m_buffer.data();
      return p[index / sizeof(long)];
    } else {
      throw std::runtime_error("error: getting the timestamp from partial window buffer of this type is not implemented");
    }
  }

  void putLong(size_t index, long value) {
    if (m_type == 0) {
      auto p = (long *) m_buffer.data();
      p[index / sizeof(long)] = value;
    } else {
      throw std::runtime_error("error: storing the timestamp to a partial window buffer of this type is not implemented");
    }
  }

  void swap(std::shared_ptr<PartialWindowResults> &partialWindowResults, int windowId) {
    auto windowBufPtr = this->getBufferPtrs()[windowId];
    auto emptyBufPtr = partialWindowResults->getBufferPtrs()[0];
    auto tempBufPtr = windowBufPtr;
    this->getBufferPtrs()[windowId] = emptyBufPtr;
    partialWindowResults->getBufferPtrs()[0] = tempBufPtr;
  }

  ~PartialWindowResults() = default;
  /*{
    for (size_t i = 0; i < bufferPtrs.size(); ++i) {
        _mm_free(bufferPtrs[i]);
    }
  }*/
};