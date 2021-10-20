#pragma once

#include <vector>

#include "utils/SystemConf.h"

/*
 * \brief UnboundedQueryBuffer is used for stateless operations, such as filtering and projections.
 *
 * */

class UnboundedQueryBuffer {
 private:
  int m_id;
  size_t m_capacity;
  size_t m_position;
  ByteBuffer m_buffer;

 public:
  UnboundedQueryBuffer(int id, size_t capacity) : m_id(id), m_capacity(capacity),
                                                  m_position(0L), m_buffer(capacity) {}

  int getBufferId() {
    return m_id;
  }

  ByteBuffer &getBuffer() {
    return m_buffer;
  }

  size_t getCapacity() {
    return m_capacity;
  }

  void setPosition(size_t pos) {
    m_position = pos;
  }

  void clear() {
    std::fill(m_buffer.begin(), m_buffer.end(), 0);
  }

  size_t getPosition() {
    return m_position;
  }

  long getLong(size_t index) {
    auto p = (long *) m_buffer.data();
    return p[index];
  }

  void putLong(size_t index, long value) {
    auto p = (long *) m_buffer.data();
    p[index] = value;
  }

  void putBytes(char *value, size_t length) {
    if (m_position + length > m_capacity) {
      throw std::runtime_error("error: increase the size of the UnboundedQueryBuffer");
    }
    std::memcpy(m_buffer.data() + m_position, value, length);
    m_position += length;
  }
};