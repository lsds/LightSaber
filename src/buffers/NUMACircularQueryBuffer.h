#pragma once

#include <atomic>
#include <vector>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <numa.h>

#include "utils/Utils.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"
#include "buffers/QueryBuffer.h"

/*
 * \brief This class implements a NUMA-aware circular buffer.
 *
 * It does so by wrapping the functionality of a circular buffer around multiple buffers that
 * are instantiated in different available memory regions.
 *
 * */

class NUMACircularQueryBuffer : public QueryBuffer {
 private:
  const int m_cpusPerNode;
  const int m_numaNodes;
  const size_t m_maxBufferCapacity;
  size_t m_minBufferCapacity;
  std::vector<std::unique_ptr<ByteBuffer>> m_buffers;
  std::vector<int> m_orderedCpus;

 public:
  NUMACircularQueryBuffer(int id, size_t capacity, int tupleSize = 1, bool copyDataOnInsert = true) :
      QueryBuffer(id, capacity, true, tupleSize, copyDataOnInsert),
#if defined(HAVE_NUMA)
      m_cpusPerNode(Utils::getNumberOfCoresPerSocket()),
      m_numaNodes(
          (numa_available() < 0) ? 1 :
          (int) std::ceil(((double) SystemConf::getInstance().WORKER_THREADS + 1) / m_cpusPerNode)),
#else
  m_cpusPerNode(SystemConf::getInstance().THREADS),
  m_numaNodes(1),
#endif
      m_maxBufferCapacity(Utils::getPowerOfTwo(m_capacity / m_numaNodes)),
      m_buffers(m_numaNodes) {
    assert(m_maxBufferCapacity % tupleSize == 0 && "Buffer capacity has to be divisible by the tuple size.");
#if defined(HAVE_NUMA)
    int numa_node = -1;
    long bufferSizePerThread = (long) (m_maxBufferCapacity / 8);
    size_t totalSize = 0;
    Utils::getOrderedCores(m_orderedCpus);
    for (int i = 0; i < m_numaNodes; ++i) {
      Utils::bindProcess(m_orderedCpus[i*m_cpusPerNode]);
      long bufferSize = (i != m_numaNodes-1) ? (long) m_maxBufferCapacity :
                        (long) ((SystemConf::getInstance().WORKER_THREADS - i * m_cpusPerNode + 1) % m_cpusPerNode)
                            * bufferSizePerThread;
      if (bufferSize == 0)
        bufferSize = m_maxBufferCapacity;
      m_buffers[i] = std::make_unique<ByteBuffer>(bufferSize);
      m_buffers[i]->data()[0] = 0;
      std::cout << "[DBG] Creating ByteBuffer " << i << std::endl;
      SystemConf::getInstance().findMemoryNodeForAddress(m_buffers[i].get()->data(), numa_node);
      std::cout << "[DBG] ByteBuffer " << i << " is allocated in " << numa_node << " numa node." << std::endl;
      /*if (numa_node != i) {
          //throw std::runtime_error("ByteBuffers are not properly allocated.");
          //numaNodes--;
          //break;
      }*/
      totalSize += bufferSize;
      m_minBufferCapacity = bufferSize;
    }
    setMask(totalSize);
    setCapacity(totalSize);
#else
    m_buffers[0] = std::make_unique<ByteBuffer>(m_maxBufferCapacity);
#endif
  };

  long put(char *values, long bytes, long latencyMark = -1) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    assert((long) m_maxBufferCapacity > bytes && "Buffer capacity has to be at least "
                                                 "the number of bytes writter to avoid spilling to more than two buffers");

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

    long index = normaliseNotPowerOfTwo(end);
    if (m_copyDataOnInsert || m_wraps == 0) { // copy only until the buffer is filled once
      int bufferIdx = getBufferIndex(index);
      /* TODO: fix the case when the batch spills to multiple buffers */
      if (bytes > ((long) m_capacity - index)) { /* Copy in two parts */
        throw std::runtime_error("error: batches should not be spilled to more than one buffer");
      } else if (bytes > ((long) ((bufferIdx + 1) * m_maxBufferCapacity) - index)) { /* Copy in two parts */
        throw std::runtime_error("error: batches should not be spilled to more than one buffer");
      } else {
        long normIndex = normaliseIndex(index, bufferIdx);
        std::memcpy(&m_buffers[bufferIdx].get()->data()[normIndex], values, (bytes) * sizeof(char));
      }
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      int bufferIdx = getBufferIndex(index);
      long normIndex = normaliseIndex(index, bufferIdx);
      long *buf = (long *) &m_buffers[bufferIdx].get()->data()[normIndex];
      buf[0] = Utils::pack(latencyMark, (int) buf[0]);
    }

    //long p = normalise(_end + bytes);
    long p = normaliseNotPowerOfTwo(end + bytes);
    if (p <= index)
      m_wraps++;
    m_endP.m_value.store(end + bytes, std::memory_order_relaxed);
    // debug ();
    return index;
  }

  void free() override {
    long _start = m_startP.m_value.load(std::memory_order_relaxed);
    long offset = normaliseNotPowerOfTwo(SystemConf::getInstance().BATCH_SIZE + _start);
    long index = normaliseNotPowerOfTwo(_start);
    long bytes;
    /* Measurements */
    if (offset <= index)
      bytes = (long) (m_capacity - index + offset + 1);
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
    long index = normaliseNotPowerOfTwo(_start);
    long bytes;
    /* Measurements */
    if (offset <= index)
      bytes = (long) (m_capacity - index + offset + 1);
    else
      bytes = offset - index + 1;

    m_bytesProcessed.fetch_add(bytes, std::memory_order_relaxed);
    m_tuplesProcessed.fetch_add((bytes / (size_t) m_tupleSize), std::memory_order_relaxed);
    m_tasksProcessed.fetch_add(1, std::memory_order_relaxed);

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
  }

  int getBufferIndex(long index) {
    return (int) (index / m_maxBufferCapacity);
  }

  long normaliseIndex(long index) {
    return (index % m_maxBufferCapacity);
  }

  long normaliseIndex(long index, int bufferIdx) {
    //return index % maxBufferCapacity;
    return (bufferIdx != m_numaNodes - 1) ? (index % m_maxBufferCapacity) : (index % m_minBufferCapacity);
  }

  ByteBuffer &getBuffer(long index) {
    //index = normalise(index);
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return getBuffer(bufferIdx);
  }

  ByteBuffer &getBuffer() override {
    return *m_buffers[0].get();
  }

  ByteBuffer &getBuffer(int bufferIdx = 0) {
    return *m_buffers[bufferIdx].get();
  }

  char *getBufferRaw(long index) {
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return getBufferRaw(bufferIdx);
  }

  char *getBufferRaw(int bufferIdx) {
    return m_buffers[bufferIdx].get()->data();
  }

  char *getBufferRaw() override {
    return m_buffers[0].get()->data();
  }

  long getLong(size_t index) override {
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    long normIndex = normaliseIndex(index, bufferIdx);
    auto p = (long *) m_buffers[bufferIdx].get()->data();
    return p[normIndex / sizeof(long)];
  }

  void setLong(size_t index, long value) override {
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    long normIndex = normaliseIndex(index, bufferIdx);
    auto p = (long *) m_buffers[bufferIdx].get()->data();
    p[normIndex / sizeof(long)] = value;
  }

  void appendBytesTo(int startPos, int endPos, ByteBuffer &outputBuffer) override {
    (void) startPos;
    (void) endPos;
    (void) outputBuffer;
    throw std::runtime_error("error: this method is not supported for the NUMA-aware Circular Buffer");
  }

  void appendBytesTo(int startPos, int endPos, char *output) override {
    (void) startPos;
    (void) endPos;
    (void) output;
    throw std::runtime_error("error: this method is not supported for the NUMA-aware Circular Buffer");
  }

  size_t getBufferCapacity(int id) override {
    return (id != m_numaNodes - 1) ? m_maxBufferCapacity : m_minBufferCapacity;
  }

  int geNumaNodeWithPtr(int index) {
    index = (int) normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return bufferIdx;
  }

  ~NUMACircularQueryBuffer() override = default;
};