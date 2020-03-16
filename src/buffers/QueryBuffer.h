#pragma once

#include <atomic>
#include <vector>
#include <iostream>
#include <stdexcept>
#include <cstring>

#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"

/*
 * \brief QueryBuffer is used as a base class for implementing a lock-free circular buffer.
 *
 * A single buffer is used per input stream to store incoming tuples and acts as a
 * SPSC queue.
 *
 * */

class QueryBuffer {

 private:
  AtomicPaddedLong m_startP;
  AtomicPaddedLong m_endP;
  size_t m_capacity;
  size_t m_mask;
  unsigned long m_wraps;
  std::atomic<size_t> m_bytesProcessed;
  std::atomic<size_t> m_tuplesProcessed;
  std::atomic<size_t> m_tasksProcessed;
  PaddedLong m_temp;
  const int m_id;
  bool m_isNuma;
  const int m_tupleSize;
  bool m_copyDataOnInsert;

  friend class CircularQueryBuffer;
  friend class NUMACircularQueryBuffer;

 public:
  QueryBuffer(int id, size_t capacity, bool isNuma, int tupleSize = 1, bool copyDataOnInsert = true) :
      m_startP(0L), m_endP(0L), m_capacity(upper_power_of_two(capacity)),
      m_mask(m_capacity - 1), m_wraps(0),
      m_bytesProcessed(0L), m_tuplesProcessed(0L), m_tasksProcessed(0L),
      m_temp(0), m_id(id), m_isNuma(isNuma), m_tupleSize(tupleSize), m_copyDataOnInsert(copyDataOnInsert) {};

  size_t upper_power_of_two(size_t v) {
    size_t power = 1;
    while (power < v)
      power *= 2;
    return power;
  }

  virtual long put(char *values, long bytes, long latencyMark = -1) = 0;

  virtual void free() = 0;

  virtual void free(long offset) = 0;

  long normalise(long index) {
    return (index & m_mask);
  }

  long normaliseNotPowerOfTwo(long index) {
    return (index % m_mask);
  }

  int getBufferId() {
    return m_id;
  }

  virtual ByteBuffer &getBuffer() = 0;

  unsigned long getWraps() {
    return m_wraps;
  }

  size_t getMask() {
    return m_mask;
  }

  size_t getCapacity() {
    return m_capacity;
  }

  virtual size_t getBufferCapacity(int id) = 0;

  size_t getBytesProcessed() {
    return m_bytesProcessed.load(std::memory_order_relaxed);
  }

  size_t getTuplesProcessed() {
    return m_tuplesProcessed.load(std::memory_order_relaxed);
  }

  size_t getTasksProcessed() {
    return m_tasksProcessed.load(std::memory_order_relaxed);
  }

  void debug() {
    long head = m_startP.m_value.load(std::memory_order_relaxed);
    long tail = m_endP.m_value.load(std::memory_order_relaxed);
    long remaining = (tail < head) ? (head - tail) : (m_capacity - (tail - head));

    std::cout << "[DBG]: start " + std::to_string(head) + " end " + std::to_string(tail) +
        " wraps " + std::to_string(m_wraps) + " " + std::to_string(remaining) << std::endl;
  }

  virtual long getLong(size_t index) = 0;

  virtual void setLong(size_t index, long value) = 0;

  virtual void appendBytesTo(int startPos, int endPos, ByteBuffer &outputBuffer) = 0;

  virtual void appendBytesTo(int startPos, int endPos, char *output) = 0;

  bool getIsNumaWrapper() {
    return m_isNuma;
  }

  virtual ~QueryBuffer() = default;

 private:
  void setMask(const size_t mask) {
    m_mask = mask;
  }

  void setCapacity(const size_t capacity) {
    m_capacity = capacity;
  }
};