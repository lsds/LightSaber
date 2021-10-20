#pragma once

#include <atomic>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "buffers/QueryBuffer.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"
#include "utils/Utils.h"

/*
 * \brief This class implements a non-NUMA-aware circular buffer.
 *
 * */

class CircularQueryBuffer : public QueryBuffer {
 private:
  ByteBuffer m_buffer;

 public:
  CircularQueryBuffer(int id, size_t capacity, int tupleSize = 1,
                      bool copyDataOnInsert = true, size_t batchSize = SystemConf::getInstance().BATCH_SIZE, bool clearFiles = true)
      : QueryBuffer(id, capacity, false, tupleSize, copyDataOnInsert, batchSize, nullptr, clearFiles),
        m_buffer(m_capacity){
          if (SystemConf::getInstance().LINEAGE_ON) {
            setupForCheckpoints(nullptr);
          }
        };


  long put(char *values, long bytes, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer " + std::to_string(m_id));

      /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint) {

      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint) {
        //std::cout << "[DBG] Circular Buffer " << std::to_string(m_id)
        //          << " is full with startP "
        //          << m_startP.m_value.load() << " and endP "
        //          << m_endP.m_value.load() << std::endl;
        return -1;
      }
    }

    long index = normalise(end);

    if (SystemConf::getInstance().LINEAGE_ON) {
      auto firstSlotId = index / m_batchSize;
      auto endSlotId = (index + bytes) / m_batchSize;
      auto slotId = endSlotId; //firstSlotId;
      while (slotId <= endSlotId) {
        auto normSlot = slotId % m_numberOfSlots;
        if (slotId != endSlotId) {
          std::lock_guard<std::mutex> l (m_slots[normSlot].m_updateLock);
          if (!m_slots[normSlot].m_graph) {
            auto newGraph = LineageGraphFactory::getInstance().newInstance();
            m_slots[normSlot].setLineageGraph(newGraph);
          }
        } else {
          std::lock_guard<std::mutex> l (m_slots[normSlot].m_updateLock);
          if (!graph && !m_slots[normSlot].m_graph) {
            graph = LineageGraphFactory::getInstance().newInstance();
          }
          if (m_slots[normSlot].m_graph && graph) {
            m_slots[normSlot].m_graph->mergeGraphs(graph);
          } else if (graph) {
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

    if (m_copyDataOnInsert ||
        m_wraps == 0) {  // copy only until the buffer is filled once
      if (bytes > ((long)m_capacity - index)) { /* Copy in two parts */
        long right = m_capacity - index;
        long left = bytes - (m_capacity - index);
        std::memcpy(&m_buffer[index], values, (right) * sizeof(char));
        std::memcpy(&m_buffer[0], &values[m_capacity - index],
                    (left) * sizeof(char));
      } else {
        std::memcpy(&m_buffer[index], values, (bytes) * sizeof(char));
      }
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    long p = normalise(end + bytes);
    if (p <= index) m_wraps++;
    m_endP.m_value.store(end + bytes, std::memory_order_relaxed);

    // debug ();
    return index;
  }

  long put(std::shared_ptr<UnboundedQueryBuffer> &input, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    auto values = input->getBuffer().data();
    auto bytes = input->getBuffer().size();

    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer " + std::to_string(m_id));

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint) {

      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint) {
        //std::cout << "[DBG] Circular Buffer " << std::to_string(m_id)
        //          << " is full with startP "
        //          << m_startP.m_value.load() << " and endP "
        //          << m_endP.m_value.load() << std::endl;
        return -1;
      }
    }

    long index = normalise(end);

    if (SystemConf::getInstance().LINEAGE_ON) {
      throw std::runtime_error("error: lineage not supported during insertion");
    }

    // always copy
    if (bytes > ((long)m_capacity - index)) { /* Copy in two parts */
      long right = m_capacity - index;
      long left = bytes - (m_capacity - index);
      std::memcpy(&m_buffer[index], values, (right) * sizeof(char));
      std::memcpy(&m_buffer[0], &values[m_capacity - index],
                  (left) * sizeof(char));
    } else {
      std::memcpy(&m_buffer[index], values, (bytes) * sizeof(char));
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    long p = normalise(end + bytes);
    if (p <= index) m_wraps++;
    m_endP.m_value.store(end + bytes, std::memory_order_relaxed);

    // free UnboundedQueryBuffer
    UnboundedQueryBufferFactory::getInstance().freeNB(input->getBufferId(), input);

    // debug ();
    return index;
  }

  long put(void *val, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    auto *values = (infinity::core::receive_element_t *) val;
    if (!values) {
      throw std::runtime_error("error: values is not set");
    }

    auto bytes = values->buffer->getSizeInBytes();
    if (bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer " + std::to_string(m_id));

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint) {

      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint) {
        //std::cout << "[DBG] Circular Buffer " << std::to_string(m_id)
        //          << " is full with startP "
        //          << m_startP.m_value.load() << " and endP "
        //          << m_endP.m_value.load() << std::endl;
        return -1;
      }
    }

    long index = normalise(end);

    if (SystemConf::getInstance().LINEAGE_ON) {
      throw std::runtime_error("error: lineage not supported during insertion");
    }

    // always copy
    if (bytes > ((long)m_capacity - index)) { /* Copy in two parts */
      long right = m_capacity - index;
      long left = bytes - (m_capacity - index);
      std::memcpy(&m_buffer[index], values->buffer->getData(), (right) * sizeof(char));
      std::memcpy(&m_buffer[0], (char*)values->buffer->getData() + (m_capacity - index),
                  (left) * sizeof(char));
    } else {
      size_t idx = 0;
      if (m_filter) {
        m_filterFP((char*) values->buffer->getData(), 0, (int) bytes, &m_buffer[index], 0, reinterpret_cast<int &>(idx));
      } else {
        std::memcpy(&m_buffer[index], values->buffer->getData(), (bytes) * sizeof(char));
      }
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    long p = normalise(end + bytes);
    if (p <= index) m_wraps++;
    m_endP.m_value.store(end + bytes, std::memory_order_relaxed);

    //
    RDMABufferPool::getInstance().free(values);

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
    m_tuplesProcessed.fetch_add((bytes / (size_t)m_tupleSize),
                                std::memory_order_relaxed);
    m_tasksProcessed.fetch_add(1, std::memory_order_relaxed);

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
  }

  void free(long offset, bool isPersistent = false) override {
    long _start = m_startP.m_value.load(std::memory_order_relaxed);
    long index = normalise(_start);
    long bytes;
    /* Measurements */
    if (offset <= index)
      bytes = m_capacity - index + offset + 1;
    else
      bytes = offset - index + 1;

    if (m_filesystem) {
      //auto slotId = normalise(index + bytes) / m_batchSize;
      //auto oldVal = 3;
      //if (!m_slots[slotId].m_slot.compare_exchange_weak(oldVal, -1)) {
      //  throw std::runtime_error("error: failed to free values from slot " + std::to_string(slotId));
      //}
      //std::cout << "[DBG] free "
      //          << offset << " offset of " << slotId << " slotId with "
      //          << m_slots[slotId].m_slot.load() << " slot " << std::endl;
      //if (m_slots[slotId].m_slot.load() != -1 && m_slots[slotId].m_slot.load() != 1 &&
      //    m_slots[slotId].m_slot.load() != 3 && !m_slots[slotId].m_ready) {
      //  debugSlots();
      //  throw std::runtime_error("error: wrong value when freeing slot " +
      //      std::to_string(slotId) + " with " +
      //      std::to_string(m_slots[slotId].m_slot.load()));
      //}
      //m_slots[slotId].reset();
    }

    m_bytesProcessed.fetch_add(bytes, std::memory_order_relaxed);
    m_tuplesProcessed.fetch_add((bytes / (size_t)m_tupleSize),
                                std::memory_order_relaxed);
    m_tasksProcessed.fetch_add(1, std::memory_order_relaxed);

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
    //debug ();
  }

  ByteBuffer &getBuffer() override { return m_buffer; }

  char *getBufferRaw() override { return m_buffer.data(); }

  size_t getBufferCapacity(int id) override {
    (void)id;
    return m_capacity;
  }

  long getLong(size_t index) override {
    auto p = (long *)m_buffer.data();
    return p[normalise(index) / sizeof(size_t)];
  }

  void setLong(size_t index, long value) override {
    auto p = (long *)m_buffer.data();
    p[normalise(index) / sizeof(size_t)] = value;
  }

  void appendBytesTo(int startPos, int endPos,
                     ByteBuffer &outputBuffer) override {
    if (endPos > startPos) {
      std::copy(m_buffer.begin() + startPos, m_buffer.begin() + endPos,
                outputBuffer.begin());
    } else {
      std::copy(m_buffer.begin() + startPos, m_buffer.end(),
                outputBuffer.begin());
      std::copy(m_buffer.begin(), m_buffer.begin() + endPos,
                outputBuffer.begin() + (m_capacity - startPos));
    }
  }

  void appendBytesTo(int startPos, int endPos, char *output) override {
    if (endPos > startPos) {
      std::memcpy(output, m_buffer.data() + startPos,
                  (endPos - startPos) * sizeof(char));
    } else {
      std::memcpy(output, m_buffer.data() + startPos,
                  (m_capacity - startPos) * sizeof(char));
      std::memcpy(output + (m_capacity - startPos), m_buffer.data(),
                  (endPos) * sizeof(char));
    }
  }

  void setupForCheckpoints(std::shared_ptr<disk_t> filesystem) override {
    m_filesystem = filesystem;
    // Initialize the slots
    for (size_t slotId = 0; slotId < m_numberOfSlots; ++slotId) {
      m_slots[slotId].setId(slotId, m_batchSize, m_buffer.data() + slotId * m_batchSize);
    }
  }

  int prepareCheckpoint(long freePtr, tbb::concurrent_queue<int> &readySlots, int &firstSlot, int &lastSlot) override {
    auto endPtr = m_endP.m_value.load();
    endPtr = normalise(endPtr);
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
      auto slotId = normalise(freePtr) / m_batchSize;

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
      auto slotId = normalise(freePtr) / m_batchSize;
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

  void debugSlots(){
    for (auto &slot : m_slots) {
      if (slot.m_id == -1) break;
      std::cout << slot.m_id << " slotId " << slot.m_slot.load()
                << " slot " << std::endl;
    }
  }

  ~CircularQueryBuffer() override = default;
};