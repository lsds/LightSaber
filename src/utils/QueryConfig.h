#pragma once

#include "utils/SystemConf.h"

/*
 * \brief This class is used to provide a configuration different
 * from the global parameters.
 *
 * */

class QueryConfig {
 private:
  size_t m_circularBufferSize;
  size_t m_batchSize;
  size_t m_bundleSize;
  size_t m_hashtableSize;
  size_t m_numberOfSlots;

 public:
  QueryConfig(size_t circularBufferSize, size_t batchSize, size_t bundleSize,
              size_t hashtableSize, size_t numberOfSlots)
      : m_circularBufferSize(circularBufferSize),
        m_batchSize(batchSize),
        m_bundleSize(bundleSize),
        m_hashtableSize(hashtableSize),
        m_numberOfSlots(numberOfSlots) {}
  size_t getCircularBufferSize() { return m_circularBufferSize; }
  size_t getBatchSize() { return m_batchSize; }
  size_t getBundleSize() { return m_bundleSize; }
  size_t getHashtableSize() { return m_hashtableSize; }
  size_t getNumberOfSlots() { return m_numberOfSlots; }
};