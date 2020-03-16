#pragma once

#include <atomic>
#include <vector>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <utils/Utils.h>

#include "QueryBuffer.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"

/*
 * \brief This class implements a non-NUMA-aware circular buffer.
 *
 * */

class CircularQueryBuffer : public QueryBuffer {
 private:
  ByteBuffer m_buffer;

 public:
  CircularQueryBuffer(int id, size_t capacity, int tupleSize = 1, bool copyDataOnInsert = true) :
      QueryBuffer(id, capacity, false, tupleSize, copyDataOnInsert), m_buffer(m_capacity) {};

  long put(char *values, long bytes, long latencyMark = -1) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);
    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint) {
      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint) {
        return -1;
      }
    }

    long index = normalise(end);
    if (m_copyDataOnInsert || m_wraps == 0) { // copy only until the buffer is filled once
      if (bytes > ((long) m_capacity - index)) { /* Copy in two parts */
        long right = m_capacity - index;
        long left = bytes - (m_capacity - index);
        std::memcpy(&m_buffer[index], values, (right) * sizeof(char));
        std::memcpy(&m_buffer[0], &values[m_capacity - index], (left) * sizeof(char));
      } else {
        std::memcpy(&m_buffer[index], values, (bytes) * sizeof(char));
      }
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *) &m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int) buf[0]);
    }

    long p = normalise(end + bytes);
    if (p <= index)
      m_wraps++;
    m_endP.m_value.store(end + bytes, std::memory_order_relaxed);
    // debug ();
    return index;
  }

  void free() override {
    long _start = m_startP.m_value.load(std::memory_order_relaxed);
    long offset = normalise(SystemConf::getInstance().BATCH_SIZE + _start);
    long index = normalise(_start);
    long bytes;
    /* Measurements */
    if (offset <= index)
      bytes = m_capacity - index + offset + 1;
    else
      bytes = offset - index + 1;

    m_bytesProcessed.fetch_add(bytes, std::memory_order_relaxed);
    m_tuplesProcessed.fetch_add((bytes / (size_t) m_tupleSize), std::memory_order_relaxed);
    m_tasksProcessed.fetch_add(1, std::memory_order_relaxed);

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
  }

  void free(long offset) override {
    long _start = m_startP.m_value.load(std::memory_order_relaxed);
    long index = normalise(_start);
    long bytes;
    /* Measurements */
    if (offset <= index)
      bytes = m_capacity - index + offset + 1;
    else
      bytes = offset - index + 1;

    m_bytesProcessed.fetch_add(bytes, std::memory_order_relaxed);
    m_tuplesProcessed.fetch_add((bytes / (size_t) m_tupleSize), std::memory_order_relaxed);
    m_tasksProcessed.fetch_add(1, std::memory_order_relaxed);

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
  }

  ByteBuffer &getBuffer() override {
    return m_buffer;
  }

  size_t getBufferCapacity(int id) override {
    (void) id;
    return m_capacity;
  }

  long getLong(size_t index) override {
    auto p = (long *) m_buffer.data();
    return p[normalise(index / sizeof(long))];
  }

  void setLong(size_t index, long value) override {
    auto p = (long *) m_buffer.data();
    p[normalise(index / sizeof(long))] = value;
  }

  void appendBytesTo(int startPos, int endPos, ByteBuffer &outputBuffer) override {
    if (endPos > startPos) {
      std::copy(m_buffer.begin() + startPos, m_buffer.begin() + endPos, outputBuffer.begin());
    } else {
      std::copy(m_buffer.begin() + startPos, m_buffer.end(), outputBuffer.begin());
      std::copy(m_buffer.begin(), m_buffer.begin() + endPos, outputBuffer.begin() + (m_capacity - startPos));
    }
  }

  void appendBytesTo(int startPos, int endPos, char *output) override {
    if (endPos > startPos) {
      std::memcpy(output, &(m_buffer[startPos]), (endPos - startPos) * sizeof(char));
    } else {
      std::memcpy(output, &(m_buffer[startPos]), (m_capacity - startPos) * sizeof(char));
      std::memcpy(output + (m_capacity - startPos), &(m_buffer[0]), (endPos) * sizeof(char));
    }
  }

  ~CircularQueryBuffer() override = default;
};