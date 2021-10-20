#pragma once

#include <atomic>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "utils/Utils.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"

class Query;

/*
 * \brief NumaBuffer is used as a base class for implementing a lock-free numa-aware circular buffer.
 *
 * */

class NumaBuffer {

 protected:
  const int m_cpusPerNode;
  const int m_numaNodes;
  const size_t m_maxBufferCapacity;
  size_t m_minBufferCapacity;
  std::vector<int> m_orderedCpus;

 public:
  NumaBuffer(size_t capacity, int tupleSize = 1) :
#if defined(HAVE_NUMA)
      m_cpusPerNode(Utils::getNumberOfCoresPerSocket()),
      m_numaNodes(
          (numa_available() < 0) ? 1 :
          (int) std::ceil(((double) SystemConf::getInstance().WORKER_THREADS + 1) / m_cpusPerNode)),
#else
      m_cpusPerNode(SystemConf::getInstance().THREADS),
      m_numaNodes(1),
#endif
      m_maxBufferCapacity(Utils::getPowerOfTwo(capacity / m_numaNodes)) {
    assert(m_maxBufferCapacity % tupleSize == 0 && "Buffer capacity has to be divisible by the tuple size.");
  };

  virtual int getBufferIndex(const long index) {
    return (int) (index / m_maxBufferCapacity);
  }

  virtual long normaliseIndex(const long index) {
    return (index % m_maxBufferCapacity);
  }

  virtual long normaliseIndex(const long index, const int bufferIdx) {
    //return index % maxBufferCapacity;
    return (bufferIdx != m_numaNodes - 1) ? (index % m_maxBufferCapacity) : (index % m_minBufferCapacity);
  }

  virtual ByteBuffer &getBuffer(long index) = 0;

  virtual char *getBufferRaw(long index) = 0;

  virtual int geNumaNodeWithPtr(int index) = 0;

  virtual ByteBuffer &getBuffer(int bufferIdx = 0) = 0;

  virtual char *getBufferRaw(int bufferIdx = 0) = 0;

  virtual ~NumaBuffer() = default;
};