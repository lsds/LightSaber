#pragma once

#include <numa.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "buffers/NumaBuffer.h"
#include "buffers/QueryBuffer.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"
#include "utils/Utils.h"

/*
 * \brief This class implements a NUMA-aware circular buffer.
 *
 * It does so by wrapping the functionality of a circular buffer around multiple buffers that
 * are instantiated in different available memory regions.
 *
 * */

class NumaCircularQueryBuffer : public QueryBuffer, public NumaBuffer {
 private:
  std::vector<std::unique_ptr<ByteBuffer>> m_buffers;

 public:
  NumaCircularQueryBuffer(int id, size_t capacity, int tupleSize = 1, bool copyDataOnInsert = true,
                          size_t batchSize = SystemConf::getInstance().BATCH_SIZE, bool clearFiles = true) :
      QueryBuffer(id, capacity, true, tupleSize, copyDataOnInsert, batchSize, nullptr, clearFiles),
      NumaBuffer(capacity, tupleSize), m_buffers(m_numaNodes) {
    assert(m_maxBufferCapacity % tupleSize == 0 && "Buffer capacity has to be divisible by the tuple size.");
#if defined(HAVE_NUMA)
    int numa_node = -1;
    long bufferSizePerThread = (long) (m_maxBufferCapacity / m_cpusPerNode);
    bufferSizePerThread = Utils::getPowerOfTwo(bufferSizePerThread);
    size_t totalSize = 0;
    Utils::getOrderedCores(m_orderedCpus);
    for (int i = 0; i < m_numaNodes; ++i) {
      Utils::bindProcess(m_orderedCpus[i*m_cpusPerNode]);
      long bufferSize = (i != m_numaNodes-1) ? (long) m_maxBufferCapacity :
                        (long) ((SystemConf::getInstance().WORKER_THREADS - i * m_cpusPerNode + 1) % m_cpusPerNode)
                            * bufferSizePerThread;
      if (m_numaNodes == 1) {
        bufferSize = m_maxBufferCapacity;
      }
      if (bufferSize == 0 || bufferSize > m_maxBufferCapacity)
        bufferSize = m_maxBufferCapacity;
      bufferSize = Utils::getPowerOfTwo(bufferSize);
      m_buffers[i] = std::make_unique<ByteBuffer>(bufferSize);
      m_buffers[i]->data()[0] = 0;
      std::cout << "[DBG] Creating ByteBuffer " << i << " with " << bufferSize << " size" << std::endl;
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
    m_numberOfSlots = totalSize/m_batchSize;
#else
    m_buffers[0] = std::make_unique<ByteBuffer>(m_maxBufferCapacity);
    m_numberOfSlots = m_maxBufferCapacity/m_batchSize;
#endif

    m_emptySlots.store(m_numberOfSlots);
    //m_slots.resize(m_numberOfSlots);

    //if (m_capacity != 1 && m_capacity % m_batchSize != 0)
    //  throw std::runtime_error("error: the capacity is not a multiple of the slot size");

    if (m_capacity > 2 && m_tupleSize != 1 && SystemConf::getInstance().LINEAGE_ON) {
      setupForCheckpoints(nullptr);
    }
  };

  long put(char *values, long bytes, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    assert((long) m_maxBufferCapacity > bytes && "Buffer capacity has to be at least "
                                                 "the number of bytes writer to avoid spilling to more than two buffers");

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

    if (SystemConf::getInstance().LINEAGE_ON) {
      auto firstSlotId = index / m_batchSize;
      auto endSlotId = (index + bytes) / m_batchSize;
      auto slotId = firstSlotId;
      while (slotId <= endSlotId) {
        auto normSlot = slotId % m_numberOfSlots;
        if (slotId != endSlotId) {
          if (!m_slots[normSlot].m_graph) {
            std::lock_guard<std::mutex> l (m_slots[normSlot].m_updateLock);
            auto newGraph = LineageGraphFactory::getInstance().newInstance();
            m_slots[normSlot].setLineageGraph(newGraph);
          }
        } else {
          if (!graph && !m_slots[normSlot].m_graph) {
            graph = LineageGraphFactory::getInstance().newInstance();
          }
          if (graph) {
            std::lock_guard<std::mutex> l (m_slots[normSlot].m_updateLock);
            m_slots[normSlot].setLineageGraph(graph);
          }
        }

        /*if (!m_slots[normSlot].m_graph) {
          throw std::runtime_error(
              "error: the lineage graph is not initialized when inserting for slot " +
              std::to_string(normSlot));
        }*/
        slotId ++;
      }
    }

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

  long put(std::shared_ptr<UnboundedQueryBuffer> &input, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    auto values = input->getBuffer().data();
    auto bytes = input->getBuffer().size();

    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    assert((long) m_maxBufferCapacity > bytes && "Buffer capacity has to be at least "
                                                 "the number of bytes writer to avoid spilling to more than two buffers");

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

    if (SystemConf::getInstance().LINEAGE_ON) {
      throw std::runtime_error("error: lineage not supported during insertion");
    }

    // always copy
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

    // free UnboundedQueryBuffer
    UnboundedQueryBufferFactory::getInstance().freeNB(input->getBufferId(), input);

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

  void free(long offset, bool isPersistent = false) override {
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

  int getBufferIndex(const long index) override {
    return (int) (index / m_maxBufferCapacity);
  }

  long normaliseIndex(const long index) override {
    return (index % m_maxBufferCapacity);
  }

  long normaliseIndex(const long index, const int bufferIdx) override {
    //return index % maxBufferCapacity;
    return (bufferIdx != m_numaNodes - 1) ? (index % m_maxBufferCapacity) : (index % m_minBufferCapacity);
  }

  ByteBuffer &getBuffer(long index) override {
    //index = normalise(index);
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return getBuffer(bufferIdx);
  }

  char *getBufferRaw(long index) override {
    index = normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return getBufferRaw(bufferIdx);
  }

  ByteBuffer &getBuffer(int bufferIdx) override {
    return *m_buffers[bufferIdx].get();
  }

  char *getBufferRaw(int bufferIdx) override {
    return m_buffers[bufferIdx].get()->data();
  }

  int geNumaNodeWithPtr(int index) override {
    index = (int) normaliseNotPowerOfTwo(index);
    int bufferIdx = getBufferIndex(index);
    return bufferIdx;
  }

  ByteBuffer &getBuffer() override {
    return *m_buffers[0].get();
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

  void setupForCheckpoints(std::shared_ptr<disk_t> filesystem) override {
    if (m_capacity != 1 && m_capacity % m_batchSize != 0)
      throw std::runtime_error("error: the capacity is not a multiple of the slot size");
    m_filesystem = filesystem;
    // Initialize the slots
    for (size_t slotId = 0; slotId < m_numberOfSlots; ++slotId) {
      m_slots[slotId].setId(slotId, m_batchSize, getBufferRaw((long)(slotId * m_batchSize)));
    }
  }

  int prepareCheckpoint(long freePtr, tbb::concurrent_queue<int> &readySlots, int &firstSlot, int &lastSlot) override {
    auto endPtr = m_endP.m_value.load();
    endPtr = normaliseNotPowerOfTwo(endPtr);
    if (freePtr > endPtr)
      endPtr += m_capacity;
    int numOfSlots = 0;

    firstSlot = -1;
    lastSlot = -1;
    bool isFirst = true;
    //auto first = freePtr;
    //std::cout << "[DBG] preparing checkpoint for circular buffer "
    //                << std::to_string(m_id) << " with "
    //                << freePtr << " freePtr " << endPtr << " endPtr "
    //                << (endPtr-freePtr)/m_batchSize << " slots " << std::endl;
    while (freePtr < endPtr - (long) m_batchSize) {
      freePtr += (long) m_batchSize;
      auto slotId = normaliseNotPowerOfTwo(freePtr) / m_batchSize;

      if (isFirst) {
        firstSlot = slotId;
        isFirst = false;
      }
      lastSlot = slotId;
      //std::cout << "[DBG] preparing checkpoint for circular buffer "
      //          << std::to_string(m_id) << " with "
      //          << std::to_string(m_batchSize) << " batchSize "
      //          << std::to_string(first) << " first " << freePtr << " freePtr "
      //          << endPtr << " endPtr " << slotId << " slotId " << std::endl;
      //if (m_slots[slotId].m_slot.load() != 1 && m_slots[slotId].m_slot.load() != 3) {
      //  debugSlots();
      //  throw std::runtime_error("error: wrong value in slot " +
      //                           std::to_string(slotId) + " with " +
      //                           std::to_string(m_slots[slotId].m_slot.load()));
      //}
      m_slots[slotId].reset();
      m_slots[slotId].setPreviousSlot(3);
      m_slots[slotId].m_slot.store(5);
      m_slots[slotId].setNumberOfResults();
      readySlots.push(slotId);
      numOfSlots++;
    }
    if (numOfSlots == 0 && freePtr < endPtr) {
      freePtr += (long) m_batchSize;
      auto slotId = normaliseNotPowerOfTwo(freePtr) / m_batchSize;
      firstSlot = slotId;
      lastSlot = slotId;
      //std::cout << "[DBG] preparing checkpoint for circular buffer "
      //          << std::to_string(m_id) << " with "
      //          << std::to_string(m_batchSize) << " batchSize "
      //          << std::to_string(first) << " first " << freePtr << " freePtr "
      //          << endPtr << " endPtr " << slotId << " slotId " << std::endl;
      m_slots[slotId].reset();
      m_slots[slotId].setPreviousSlot(3);
      m_slots[slotId].m_slot.store(5);
      m_slots[slotId].setNumberOfResults();
      readySlots.push(slotId);
      numOfSlots++;
    }
    return numOfSlots;
  }

  size_t getBufferCapacity(int id) override {
    return (id != m_numaNodes - 1) ? m_maxBufferCapacity : m_minBufferCapacity;
  }

  void fixTimestamps(size_t index, long timestamp, long step, long batchSize) {
    throw std::runtime_error("error: this method is not supported for the NUMA-aware Circular Buffer");
  }

  ~NumaCircularQueryBuffer() override = default;
};