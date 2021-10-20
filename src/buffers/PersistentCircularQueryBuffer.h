#pragma once

#include <err.h>
#include <fcntl.h>
#include <libpmemobj.h>
#include <libpmemobj/pool_base.h>
#include <malloc.h>
#include <mm_malloc.h>
#include <sys/wait.h>
#include <unistd.h>
#include <xmmintrin.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr_base.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <stdexcept>
#include <utility>
#include <vector>

#include "PartialWindowResultsFactory.h"
#include "UnboundedQueryBufferFactory.h"
#include "buffers/QueryBuffer.h"
#include "tasks/Task.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Async.h"
#include "utils/PaddedLong.h"
#include "utils/Query.h"
#include "utils/SystemConf.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"
#include "utils/WindowDefinition.h"

/*
 * \brief This class implements a non-NUMA-aware circular buffer
 * that persists data to disk asynchronously. It used to implement a P-Stream.
 *
 * */

class AckSlotContext : public IAsyncContext {
 public:
  AckSlotContext(std::atomic<int> *slot, std::shared_ptr<PartialWindowResults> parW = nullptr) : m_slot(slot), m_parW(parW) {}

 protected:
  Status deepCopyInternal(IAsyncContext *&context_copy) final {
    return IAsyncContext::deepCopyInternal(*this, context_copy);
  }

 public:
  std::atomic<int> *m_slot;
  std::shared_ptr<PartialWindowResults> m_parW;
};

class PersistentCircularQueryBuffer : public QueryBuffer {
 private:
  struct PMem;

  /// The buffer holding the in-memory data
  ByteBuffer m_buffer;

  // todo: these have to be persisted
  std::atomic<size_t> m_nextFreeSlot;
  /* Lock protecting the acknowledgment of persisting the input to a file */
  std::mutex m_ackLock;
  std::atomic<size_t> m_nextPersistentSlot;

  // Variables for persisting the file pointers
  const size_t m_poolSize;
  const std::string m_layout = "";
  pmem::obj::pool<PMem> m_pop;
  pmem::obj::persistent_ptr<PMem> m_root;
  std::string m_pmFileName;
  file_t *m_pmFile;

  // Variables for persisting asynchronously the actual data
  std::string m_asyncFileName;
  file_t *m_asyncFile;
  FileOptions m_asyncFileOptions;

  std::atomic<unsigned long> m_wraps = 0;

  // Used for compression
  std::vector<ByteBuffer> m_copyBuffers;

  bool m_ready = false;
  const bool m_debug = false;

 public:
  PersistentCircularQueryBuffer(int id, size_t capacity, int tupleSize = 1,
                                bool copyDataOnInsert = true, size_t batchSize = SystemConf::getInstance().BATCH_SIZE,
                                std::shared_ptr<disk_t> filesystem = nullptr, bool clearFiles = true,
                                bool unbuffered = true, bool delete_on_close = false)
      : QueryBuffer(id, capacity, false, tupleSize, copyDataOnInsert, batchSize, filesystem, clearFiles),
        m_buffer(capacity),
        m_nextFreeSlot(0),
        m_nextPersistentSlot(0),
        m_poolSize(PMEMOBJ_MIN_POOL),
        m_pmFileName("scabbard/queue_pm_" + std::to_string(id)),
        m_asyncFileName("scabbard/queue_data_" + std::to_string(id)),
        m_asyncFileOptions(unbuffered, delete_on_close),
        m_copyBuffers(SystemConf::getInstance().WORKER_THREADS, ByteBuffer(m_batchSize)) {
    if (m_capacity % m_batchSize != 0)
      throw std::runtime_error("error: the capacity is not a multiple of the slot size");
    if (!(m_numberOfSlots && !(m_numberOfSlots & (m_numberOfSlots - 1)))) {
        throw std::runtime_error ("error: the number of slots has to be a power of two");
    }
    try {
      if (!m_filesystem) {
        std::cout << "warning: no filesystem passed to the constructor. "
                     "Initializing a new filesystem..." << std::endl;
        m_filesystem = std::make_shared<disk_t>(SystemConf::FILE_ROOT_PATH, SystemConf::getInstance().WORKER_THREADS);
      }

      Utils::tryCreateDirectory(m_filesystem->getRootPath() + "scabbard");
      auto pmPath = m_filesystem->getRootPath() + m_pmFileName;
      if (Utils::fileExists(pmPath.c_str()) != 0) {
        m_pop = pmem::obj::pool<PMem>::create(pmPath.c_str(),
                                              "", m_poolSize, CREATE_MODE_RW);
        m_root = m_pop.root();
        pmem::obj::make_persistent_atomic<PMem>(m_pop, m_root->next);
        pmem::obj::transaction::run(m_pop, [&] { m_root = m_root->next; });
        m_previousBlockSize = SystemConf::getInstance().BATCH_SIZE;
      } else {
        m_pop = pmem::obj::pool<PMem>::open(pmPath, "");
        m_root = m_pop.root();
        m_root = m_root->next;
        m_previousBlockSize = m_root->m_blockSize.get_ro();
      }
    } catch (const pmem::pool_error &e) {
      std::cerr << "Exception: " << e.what() << std::endl;
      return;
    } catch (const pmem::transaction_error &e) {
      std::cerr << "Exception: " << e.what() << std::endl;
      return;
    }

    m_root->m_blockSize.get_rw() = SystemConf::getInstance().BLOCK_SIZE;

    // Initialize the slots
    for (size_t slotId = 0; slotId < m_numberOfSlots; ++slotId) {
      m_slots[slotId].setId(slotId, m_batchSize, nullptr);
    }

    // Open File handlers
    m_pmFile = m_filesystem->newFile(m_pmFileName); // do I need this?
    if (!SystemConf::getInstance().LINEAGE_ON) {
      m_asyncFile = m_filesystem->newFile(m_asyncFileName, m_numberOfSlots * m_batchSize);
    }
    m_ready = true;
  };

  long put(char *values, long bytes, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    while (!m_ready)
      ;

    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    if (bytes != m_batchSize)
      throw std::invalid_argument("error: the size of the input must be equal to the slot ("+std::to_string(m_batchSize)+" != "+std::to_string(bytes)+")");

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    bool hasEmptySlots = (m_emptySlots > 0);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
         // std::cout << "[DBG] Circular Buffer is full with startP "
         //          << m_startP.m_value.load() << " and endP "
         //          << m_endP.m_value.load() << std::endl;
        // check if some async calls have finished
        m_filesystem->getHandler().tryCompleteMultiple();
        tryToAcknowledge();
        return -1;
      }
    }

    // create task
    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Creating insertion task for slot "
                + std::to_string(m_nextSlotToWrite)
                + " with empty slots " + std::to_string(m_emptySlots.load()-1) << std::endl;
    }
    createInsertionTask(values, bytes, latencyMark, retainMark, graph);
    m_emptySlots.fetch_add(-1);
    // try to forward the end pointer
    m_filesystem->getHandler().tryCompleteMultiple();
    tryToAcknowledge();
    //long index = tryConsumeNextSlot();

    if (m_debug) {
      debug();
    }
    return 0;
  }

  long put(std::shared_ptr<UnboundedQueryBuffer> &values, long latencyMark, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    while (!m_ready)
      ;

    auto bytes = values->getBuffer().size();
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    if (bytes != m_batchSize)
      throw std::invalid_argument("error: the size of the input must be equal to the slot ("+std::to_string(m_batchSize)+" != "+std::to_string(bytes)+")");

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    bool hasEmptySlots = (m_emptySlots > 0);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
        // std::cout << "[DBG] Circular Buffer is full with startP "
        //          << m_startP.m_value.load() << " and endP "
        //          << m_endP.m_value.load() << std::endl;
        // check if some async calls have finished
        m_filesystem->getHandler().tryCompleteMultiple();
        tryToAcknowledge();
        return -1;
      }
    }

    // create task
    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Creating insertion task for slot "
          + std::to_string(m_nextSlotToWrite)
          + " with empty slots " + std::to_string(m_emptySlots.load()-1) << std::endl;
    }
    createInsertionTask(values, bytes, latencyMark, retainMark, graph);
    m_emptySlots.fetch_add(-1);
    // try to forward the end pointer
    m_filesystem->getHandler().tryCompleteMultiple();
    tryToAcknowledge();
    //long index = tryConsumeNextSlot();

    if (m_debug) {
      debug();
    }
    return 0;
  }

  long put(void *val, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) override {
    while (!m_ready)
      ;

    infinity::core::receive_element_t *values = (infinity::core::receive_element_t *) val;
    if (!values) {
      throw std::runtime_error("error: values is not set");
    }

    auto bytes = values->buffer->getSizeInBytes();
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");
    if (bytes != m_batchSize)
      throw std::invalid_argument("error: the size of the input must be equal to the slot ("+std::to_string(m_batchSize)+" != "+std::to_string(bytes)+")");

    /* Get the end pointer */
    long end = m_endP.m_value.load(std::memory_order_relaxed);

    bool hasEmptySlots = (m_emptySlots > 0);

    /* Find remaining bytes until the circular buffer wraps */
    long wrapPoint = (end + bytes - 1) - m_capacity;
    if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
      m_temp.m_value = m_startP.m_value.load(std::memory_order_relaxed);
      if (m_temp.m_value <= wrapPoint || !hasEmptySlots) {
        // std::cout << "[DBG] Circular Buffer is full with startP "
        //          << m_startP.m_value.load() << " and endP "
        //          << m_endP.m_value.load() << std::endl;
        // check if some async calls have finished
        m_filesystem->getHandler().tryCompleteMultiple();
        tryToAcknowledge();
        return -1;
      }
    }

    // create task
    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Creating insertion task for slot "
          + std::to_string(m_nextSlotToWrite)
          + " with empty slots " + std::to_string(m_emptySlots.load()-1) << std::endl;
    }
    createInsertionTask(values, bytes, latencyMark, retainMark, graph);
    m_emptySlots.fetch_add(-1);
    // try to forward the end pointer
    m_filesystem->getHandler().tryCompleteMultiple();
    tryToAcknowledge();
    //long index = tryConsumeNextSlot();

    if (m_debug) {
      debug();
    }
    return 0;

  }

  void putRows(int pid, char *values, long bytes, size_t slot, long latencyMark, long retainMark, int wraps) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");

    auto id = slot;
    auto slotId = id % m_numberOfSlots;
    //m_slots[slotId].m_slot.store(0);
    while (m_slots[slotId].m_slot.load() != 0) {
      std::cout << "error: inserting data to slot " << slotId
                << " is blocked with oldVal " << m_slots[slotId].m_slot.load() << std::endl;
      exit(1);
      _mm_pause();
    }

    auto index = slotId * m_batchSize;

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker writing to slot " + std::to_string(slotId)
                + " with " + std::to_string(index) + " startPointer"
                + " and " + std::to_string(index+bytes) + " endPointer " << std::endl;
    }

    // check if some async calls have finished
    // m_filesystem->getHandler().tryCompleteMultiple();
    // tryToAcknowledge();

    if (!retainMark) {
      if (SystemConf::getInstance().LINEAGE_ON) {
        if (!m_slots[slotId].m_graph)
          throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
        auto bufferId = (m_id % 2 == 0) ? 0 : 1;
        auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      }
      auto oldVal = 0;
      m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 1);
      // check if some async calls have finished
      m_filesystem->getHandler().tryCompleteMultiple();
      std::cout << "warning: no retain marker was used" << std::endl;
      return;
    }

    // async write here
    auto copyBuffer = PartialWindowResultsFactory::getInstance().newInstance(pid);
    AckSlotContext context{&m_slots[slotId].m_slot, copyBuffer};
    auto callback = [](IAsyncContext *ctxt, Status result,
                       size_t bytes_transferred) {
      CallbackContext<AckSlotContext> context{ctxt};
      if (result != Status::Ok) {
        fprintf(stderr, "AsyncFlushPages(), error: %u\n",
                static_cast<uint8_t>(result));
      }

      // std::cout << "[DBG] callback setting the slot status with "
      //          << bytes_transferred << " bytes_transferred" << std::endl;
      // Set the slot status to ready
      auto oldVal = 0;
      while (!context->m_slot->compare_exchange_weak(oldVal, 1)) {
        std::cout << "warning: callback (" << std::this_thread::get_id()
                  << ") blocked with oldVal " << oldVal << std::endl;
        _mm_pause();
      }
      if (context->m_parW) {
        PartialWindowResultsFactory::getInstance().free(context->m_parW->getThreadId(), context->m_parW);
        context->m_parW.reset();
      }
    };

    if (m_copyDataOnInsert || wraps == 0) {  // copy only until the buffer is filled once
      std::memcpy(&m_buffer[index], values, bytes);
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    // memcpy may succeed after the write on disk in a multi-threaded scenario!
    m_slots[slotId].m_memcpyFinished.store(true);

    int diskBytes = 0;
    char *diskValues = &m_buffer[index];
    bool clear = false;
    if (m_compress) {
      if (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON && m_startInstr && id % 128 == 0) {
        // results
        uint32_t *dVs = nullptr;
        double *cVs  = nullptr, *mns  = nullptr, *mxs  = nullptr, *mxDs  = nullptr;
        m_instrFP(pid, values, (int) bytes, dVs, cVs, mns, mxs, mxDs);
        m_compStats->addStatistics(dVs, cVs, mns, mxs, mxDs);
      }
      int metadataSize = 128;
      if (pid >= m_copyBuffers.size())
        throw std::runtime_error("error: invalid pid for data insertions with compression");

      //m_compressionFP[m_compPos](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
      m_compressionFP[m_compPos](pid, values, 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
      // if compression fails, fall back to the initial compression scheme
      if (clear) {
        //std::cout << "[DBG] falling back to the initial compression scheme" << std::endl;
        //m_compressionFP[0](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
        m_compressionFP[0](pid, values, 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
        m_compPos = 0;
      }
      diskBytes += metadataSize;
      latencyMark = (SystemConf::getInstance().LATENCY_ON) ? latencyMark : -1;
      if (clear || m_compPos == 0) {
        //m_compressionFP[0](pid, values, 0, -1, m_copyBuffers[pid].data(), diskBytes, (int) m_copyBuffers[pid].size(), clear, latencyMark);
        m_compressionFP[0](pid, values, 0, -1, copyBuffer->getBufferRaw(), diskBytes, (int) copyBuffer->getCapacity(), clear, latencyMark);
      }
      //diskValues = m_copyBuffers[pid].data();
      diskValues = copyBuffer->getBufferRaw();

      m_storedBytes.fetch_add(diskBytes);
      m_storedCounter.fetch_add(1);
      //diskBytes = 64 * 1024;//bytes;
    } else {
      diskBytes = bytes;
    }
    diskBytes = roundOffset(Utils::getPowerOfTwo(diskBytes));
#if defined(NO_DISK)
    diskBytes = 0;
#endif
    m_root->updateBlockSize(diskBytes);

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker compressing data for slot " + std::to_string(slotId)
                + " of " + std::to_string(bytes) + " bytes to "
                + std::to_string(diskBytes) + " with "
                + std::to_string((double)bytes/(double)diskBytes) + " ratio " << std::endl;
    }

    if (diskBytes > m_batchSize)
      throw std::runtime_error("error: the write exceeds the size of slots in the input log");
    if (!SystemConf::getInstance().LINEAGE_ON) {
      assert(m_asyncFile->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                                  slotId * m_batchSize, diskBytes, callback,
                                  context) == Status::Ok);
    } else {
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      assert(fptr->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                                     slotId * m_batchSize, diskBytes, callback,
                                     context) == Status::Ok);
    }
    // copyBuffer.reset();

    // check if some async calls have finished
    m_filesystem->getHandler().tryCompleteMultiple();
    //tryToAcknowledge();
  }

  void putRows(int pid, std::shared_ptr<UnboundedQueryBuffer> &values, long bytes, size_t slot, long latencyMark, long retainMark, int wraps) override {
    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");

    auto id = slot;
    auto slotId = id % m_numberOfSlots;
    //m_slots[slotId].m_slot.store(0);
    while (m_slots[slotId].m_slot.load() != 0) {
      std::cout << "error: inserting data to slot " << slotId
                << " is blocked with oldVal " << m_slots[slotId].m_slot.load() << std::endl;
      exit(1);
      _mm_pause();
    }

    auto index = slotId * m_batchSize;

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker writing to slot " + std::to_string(slotId)
          + " with " + std::to_string(index) + " startPointer"
          + " and " + std::to_string(index+bytes) + " endPointer " << std::endl;
    }

    // check if some async calls have finished
    // m_filesystem->getHandler().tryCompleteMultiple();
    // tryToAcknowledge();

    if (!retainMark) {
      if (SystemConf::getInstance().LINEAGE_ON) {
        if (!m_slots[slotId].m_graph)
          throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
        auto bufferId = (m_id % 2 == 0) ? 0 : 1;
        auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      }
      auto oldVal = 0;
      m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 1);
      // check if some async calls have finished
      m_filesystem->getHandler().tryCompleteMultiple();
      std::cout << "warning: no retain marker was used" << std::endl;
      return;
    }

    // async write here
    auto copyBuffer = PartialWindowResultsFactory::getInstance().newInstance(pid);
    AckSlotContext context{&m_slots[slotId].m_slot, copyBuffer};
    auto callback = [](IAsyncContext *ctxt, Status result,
                       size_t bytes_transferred) {
      CallbackContext<AckSlotContext> context{ctxt};
      if (result != Status::Ok) {
        fprintf(stderr, "AsyncFlushPages(), error: %u\n",
                static_cast<uint8_t>(result));
      }

      // std::cout << "[DBG] callback setting the slot status with "
      //          << bytes_transferred << " bytes_transferred" << std::endl;
      // Set the slot status to ready
      auto oldVal = 0;
      while (!context->m_slot->compare_exchange_weak(oldVal, 1)) {
        std::cout << "warning: callback (" << std::this_thread::get_id()
                  << ") blocked with oldVal " << oldVal << std::endl;
        _mm_pause();
      }
      if (context->m_parW) {
        PartialWindowResultsFactory::getInstance().free(context->m_parW->getThreadId(), context->m_parW);
        context->m_parW.reset();
      }
    };

    if (m_copyDataOnInsert || wraps == 0) {  // copy only until the buffer is filled once
      std::memcpy(&m_buffer[index], values->getBuffer().data(), bytes);
    }
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    // memcpy may succeed after the write on disk in a multi-threaded scenario!
    m_slots[slotId].m_memcpyFinished.store(true);

    int diskBytes = 0;
    char *diskValues = &m_buffer[index];
    bool clear = false;
    if (m_compress) {
      if (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON && m_startInstr && id % 128 == 0) {
        // results
        uint32_t *dVs = nullptr;
        double *cVs  = nullptr, *mns  = nullptr, *mxs  = nullptr, *mxDs  = nullptr;
        m_instrFP(pid, values->getBuffer().data(), (int) bytes, dVs, cVs, mns, mxs, mxDs);
        m_compStats->addStatistics(dVs, cVs, mns, mxs, mxDs);
      }
      int metadataSize = 128;
      if (pid >= m_copyBuffers.size())
        throw std::runtime_error("error: invalid pid for data insertions with compression");

      //m_compressionFP[m_compPos](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
      m_compressionFP[m_compPos](pid, values->getBuffer().data(), 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
      // if compression fails, fall back to the initial compression scheme
      if (clear) {
        //std::cout << "[DBG] falling back to the initial compression scheme" << std::endl;
        //m_compressionFP[0](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
        m_compressionFP[0](pid, values->getBuffer().data(), 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
        m_compPos = 0;
      }
      diskBytes += metadataSize;
      latencyMark = (SystemConf::getInstance().LATENCY_ON) ? latencyMark : -1;
      if (clear || m_compPos == 0) {
        //m_compressionFP[0](pid, values, 0, -1, m_copyBuffers[pid].data(), diskBytes, (int) m_copyBuffers[pid].size(), clear, latencyMark);
        m_compressionFP[0](pid, values->getBuffer().data(), 0, -1, copyBuffer->getBufferRaw(), diskBytes, (int) copyBuffer->getCapacity(), clear, latencyMark);
      }
      //diskValues = m_copyBuffers[pid].data();
      diskValues = copyBuffer->getBufferRaw();

      m_storedBytes.fetch_add(diskBytes);
      m_storedCounter.fetch_add(1);
      //diskBytes = 64 * 1024;//bytes;

      // free UnboundedQueryBuffer
      UnboundedQueryBufferFactory::getInstance().freeNB(values->getBufferId(), values);
    } else {
      diskBytes = bytes;
    }
    diskBytes = roundOffset(Utils::getPowerOfTwo(diskBytes));
#if defined(NO_DISK)
    diskBytes = 0;
#endif
    m_root->updateBlockSize(diskBytes);

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker compressing data for slot " + std::to_string(slotId)
          + " of " + std::to_string(bytes) + " bytes to "
          + std::to_string(diskBytes) + " with "
          + std::to_string((double)bytes/(double)diskBytes) + " ratio " << std::endl;
    }

    if (diskBytes > m_batchSize)
      throw std::runtime_error("error: the write exceeds the size of slots in the input log");
    if (!SystemConf::getInstance().LINEAGE_ON) {
      assert(m_asyncFile->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                                     slotId * m_batchSize, diskBytes, callback,
                                     context) == Status::Ok);
    } else {
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      assert(fptr->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                              slotId * m_batchSize, diskBytes, callback,
                              context) == Status::Ok);
    }
    // copyBuffer.reset();

    // check if some async calls have finished
    m_filesystem->getHandler().tryCompleteMultiple();
    //tryToAcknowledge();
  }

  void putRows(int pid, void *val, long bytes, size_t slot, long latencyMark, long retainMark, int wraps) override {
    auto *values = (infinity::core::receive_element_t *) val;

    if (values == nullptr || bytes <= 0)
      throw std::invalid_argument("error: cannot put null to circular buffer");

    /*for (auto ii = 0; ii < bytes; ii+=8) {
      __builtin_prefetch(((char*) values->buffer->getData() + ii), 1, 3);
    }*/

    auto id = slot;
    auto slotId = id % m_numberOfSlots;
    //m_slots[slotId].m_slot.store(0);
    while (m_slots[slotId].m_slot.load() != 0) {
      std::cout << "error: inserting data to slot " << slotId
                << " is blocked with oldVal " << m_slots[slotId].m_slot.load() << std::endl;
      exit(1);
      _mm_pause();
    }

    auto index = slotId * m_batchSize;

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker writing to slot " + std::to_string(slotId)
          + " with " + std::to_string(index) + " startPointer"
          + " and " + std::to_string(index+bytes) + " endPointer " << std::endl;
    }

    // check if some async calls have finished
    // m_filesystem->getHandler().tryCompleteMultiple();
    // tryToAcknowledge();

    if (!retainMark) {
      if (SystemConf::getInstance().LINEAGE_ON) {
        if (!m_slots[slotId].m_graph)
          throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
        auto bufferId = (m_id % 2 == 0) ? 0 : 1;
        auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      }
      auto oldVal = 0;
      m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 1);
      // check if some async calls have finished
      m_filesystem->getHandler().tryCompleteMultiple();
      std::cout << "warning: no retain marker was used" << std::endl;
      return;
    }

    // async write here
    auto copyBuffer = PartialWindowResultsFactory::getInstance().newInstance(pid);
    AckSlotContext context{&m_slots[slotId].m_slot, copyBuffer};
    auto callback = [](IAsyncContext *ctxt, Status result,
                       size_t bytes_transferred) {
      CallbackContext<AckSlotContext> context{ctxt};
      if (result != Status::Ok) {
        fprintf(stderr, "AsyncFlushPages(), error: %u\n",
                static_cast<uint8_t>(result));
      }

      // std::cout << "[DBG] callback setting the slot status with "
      //          << bytes_transferred << " bytes_transferred" << std::endl;
      // Set the slot status to ready
      auto oldVal = 0;
      while (!context->m_slot->compare_exchange_weak(oldVal, 1)) {
        std::cout << "warning: callback (" << std::this_thread::get_id()
                  << ") blocked with oldVal " << oldVal << std::endl;
        _mm_pause();
      }
      if (context->m_parW) {
        PartialWindowResultsFactory::getInstance().free(context->m_parW->getThreadId(), context->m_parW);
        context->m_parW.reset();
      }
    };

    if (m_copyDataOnInsert || wraps == 0) {  // copy only until the buffer is filled once
      //std::memcpy(&m_buffer[index], values->buffer->getData(), bytes);
      size_t idx = 0;
      if (m_filter) {
        m_filterFP((char*) values->buffer->getData(), 0, (int) bytes, &m_buffer[index], 0, reinterpret_cast<int &>(idx));
      } else {
        std::memcpy(&m_buffer[index], values->buffer->getData(), bytes);
      }
    }
    //if (wraps == 0 && !m_copyDataOnInsert) {
    //  std::cout << " I am not copying data " << std::endl;
    //}
    if (SystemConf::getInstance().LATENCY_ON && !m_copyDataOnInsert) {
      long *buf = (long *)&m_buffer[index];
      buf[0] = Utils::pack(latencyMark, (int)buf[0]);
    }

    // memcpy may succeed after the write on disk in a multi-threaded scenario!
    m_slots[slotId].m_memcpyFinished.store(true);

    int diskBytes = 0;
    char *diskValues = &m_buffer[index];
    bool clear = false;
    if (m_compress) {
      if (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON && m_startInstr && id % 128 == 0) {
        // results
        uint32_t *dVs = nullptr;
        double *cVs  = nullptr, *mns  = nullptr, *mxs  = nullptr, *mxDs  = nullptr;
        m_instrFP(pid, (char*) values->buffer->getData(), (int) bytes, dVs, cVs, mns, mxs, mxDs);
        m_compStats->addStatistics(dVs, cVs, mns, mxs, mxDs);
      }
      int metadataSize = 128;
      if (pid >= m_copyBuffers.size())
        throw std::runtime_error("error: invalid pid for data insertions with compression");

      //m_compressionFP[m_compPos](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
      m_compressionFP[m_compPos](pid, (char*) values->buffer->getData(), 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
      // if compression fails, fall back to the initial compression scheme
      if (clear) {
        //std::cout << "[DBG] falling back to the initial compression scheme" << std::endl;
        //m_compressionFP[0](pid, values, 0, (int) bytes, m_copyBuffers[pid].data() + metadataSize, diskBytes, (int) m_copyBuffers[pid].size(), clear, -1);
        m_compressionFP[0](pid, (char*) values->buffer->getData(), 0, (int) bytes, copyBuffer->getBufferRaw() + metadataSize, diskBytes, (int) copyBuffer->getCapacity(), clear, -1);
        m_compPos = 0;
      }
      diskBytes += metadataSize;
      latencyMark = (SystemConf::getInstance().LATENCY_ON) ? latencyMark : -1;
      if (clear || m_compPos == 0) {
        //m_compressionFP[0](pid, values, 0, -1, m_copyBuffers[pid].data(), diskBytes, (int) m_copyBuffers[pid].size(), clear, latencyMark);
        m_compressionFP[0](pid, (char*) values->buffer->getData(), 0, -1, copyBuffer->getBufferRaw(), diskBytes, (int) copyBuffer->getCapacity(), clear, latencyMark);
      }
      //diskValues = m_copyBuffers[pid].data();
      diskValues = copyBuffer->getBufferRaw();

      m_storedBytes.fetch_add(diskBytes);
      m_storedCounter.fetch_add(1);
      //diskBytes = 64 * 1024;//bytes;
    } else {
      diskBytes = bytes;
    }
    // free rdma buffer
    RDMABufferPool::getInstance().free(values);

    diskBytes = roundOffset(Utils::getPowerOfTwo(diskBytes));
#if defined(NO_DISK)
    diskBytes = 0;
#endif
    m_root->updateBlockSize(diskBytes);

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Worker compressing data for slot " + std::to_string(slotId)
          + " of " + std::to_string(bytes) + " bytes to "
          + std::to_string(diskBytes) + " with "
          + std::to_string((double)bytes/(double)diskBytes) + " ratio " << std::endl;
    }

    if (diskBytes > m_batchSize)
      throw std::runtime_error("error: the write exceeds the size of slots in the input log");
    if (!SystemConf::getInstance().LINEAGE_ON) {
      assert(m_asyncFile->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                                     slotId * m_batchSize, diskBytes, callback,
                                     context) == Status::Ok);
    } else {
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized for slot " + std::to_string(slotId));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_slots[slotId].m_fptr; //m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      assert(fptr->writeAsync(reinterpret_cast<const uint8_t *>(diskValues),
                              slotId * m_batchSize, diskBytes, callback,
                              context) == Status::Ok);
    }
    // copyBuffer.reset();

    // check if some async calls have finished
    m_filesystem->getHandler().tryCompleteMultiple();
    //tryToAcknowledge();
  }

  long recover(int &bytes) override {
    if (!SystemConf::getInstance().LINEAGE_ON) {
      throw std::runtime_error("error: lineage must be enabled for recovery");
    }
    if (m_numberOfSlotsToRecover == 0) {
      return -1;
    }

    if (m_emptySlots <= 0) {
      m_filesystem->getHandler().tryCompleteMultiple();
      tryToAcknowledge();
      return 0;
    }

    /* Get the slot to write */
    auto slot = getNextSlotToWrite();
    if (slot >= m_numberOfSlots)
      m_wraps = 1;
    auto slotId = slot % m_numberOfSlots;
    auto index = slotId * m_batchSize;
    auto end = (m_compress) ? slotId * SystemConf::getInstance().BLOCK_SIZE : slotId * m_batchSize;
    long readEnd = m_pop.root()->m_startP.get_ro().load() + end;
    long readIndex = normalise(readEnd);

    auto graph = LineageGraphFactory::getInstance().newInstance();

    m_slots[slotId].setLineageGraph(graph);
    auto bufferId = (m_id % 2 == 0) ? 0 : 1;
    auto fptr = m_fileStore->getUnsafeFilePtr(m_query->getId(), bufferId, readEnd, m_pop.root()->m_startId.get_ro().load());
    m_slots[slotId].m_fptr = fptr;

    AckSlotContext context{&m_slots[slotId].m_slot};
    auto callback = [](IAsyncContext *ctxt, Status result,
                       size_t bytes_transferred) {
      CallbackContext<AckSlotContext> context{ctxt};
      if (result != Status::Ok) {
        fprintf(stderr, "AsyncFlushPages(), error: %u\n",
                static_cast<uint8_t>(result));
      }

      // std::cout << "[DBG] callback setting the slot status with "
      //           << bytes_transferred << " bytes_transferred" << std::endl;
      // Set the slot status to ready
      auto oldVal = 0;
      while (!context->m_slot->compare_exchange_weak(oldVal, 1)) {
        std::cout << "warning: callback (" << std::this_thread::get_id()
                  << ") blocked with oldVal " << oldVal << std::endl;
        _mm_pause();
      }
    };

    auto oldVal = -1;
    while (!m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 0)) {
      std::cout << "warning: (during recovery) adding data to slot " << slotId
                << " is blocked with oldVal " << oldVal << std::endl;
      _mm_pause();
    }

    bytes = (!m_compress) ? m_batchSize : std::min((size_t)m_previousBlockSize, m_batchSize);
    if (m_copyDataOnInsert) {
      assert(fptr->readAsync(readIndex, &m_buffer[index], bytes,
                             callback, context) == Status::Ok);
    } else {
      //std::cout << "reading async data for slot " << slotId << std::endl;
      assert(fptr->readAsync(0, &m_buffer[index], bytes,
                             callback, context) == Status::Ok);
    }

    m_slots[slotId].m_memcpyFinished.store(true);

    // reduce number of slots
    m_numberOfSlotsToRecover--;
    m_emptySlots.fetch_add(-1);

    if (m_debug) {
      debug();
    }

    m_filesystem->getHandler().tryCompleteMultiple();
    tryToAcknowledge();
    return 0;
  }

  void prepareRecovery() override {
    if (!m_copyDataOnInsert) {
      if (m_numberOfSlotsToRecover < m_numberOfSlots) {
        long readEnd = m_pop.root()->m_startP.get_ro().load();
        long readIndex = normalise(readEnd);
        auto bufferId = (m_id % 2 == 0) ? 0 : 1;
        auto fptr = m_fileStore->getUnsafeFilePtr(m_query->getId(), bufferId, readEnd, m_pop.root()->m_startId.get_ro().load());
        if (m_compress) {
          auto sIndex = m_numberOfSlotsToRecover * m_batchSize;
          auto readBytes = std::min((size_t)m_previousBlockSize, m_batchSize);
          assert(fptr->readSync(0, m_copyBuffers[0].data(), readBytes) == Status::Ok);
          int writePos = 0;
          bool copy = false;
          auto latency = (SystemConf::getInstance().LATENCY_ON) ? 0 : -1;
          m_decompressionFP[m_compPos](0, &m_buffer[sIndex], 0, m_batchSize, m_copyBuffers[0].data(), writePos, SystemConf::getInstance().BLOCK_SIZE, copy, latency);
          if (writePos != SystemConf::getInstance().BATCH_SIZE)
            throw std::runtime_error("error: the write position is not equal to the batch size after decompression");
          //std::cout << "[DBG] decompressing for slot " << m_numberOfSlotsToRecover << " and index " << sIndex << std::endl;
          for (size_t slotId = m_numberOfSlotsToRecover + 1; slotId < m_numberOfSlots; ++slotId) {
            auto index = slotId * m_batchSize;
            std::memcpy(&m_buffer[index], &m_buffer[sIndex], m_batchSize);
            //std::cout << "[DBG] copying for slot " << slotId << " and index " << index << std::endl;
          }
        } else {
          assert(fptr->readSync(0, m_copyBuffers[0].data(), m_batchSize) == Status::Ok);
          for (size_t slotId = m_numberOfSlotsToRecover; slotId < m_numberOfSlots; ++slotId) {
            auto index = slotId * m_batchSize;
            std::memcpy(&m_buffer[index], m_copyBuffers[0].data(), m_batchSize);
          }
        }
      }
      m_wraps = 1;
    }

  }

  bool tryConsumeNextSlot(long &index, int &length, bool recover = false) {
    if (recover) {
      m_filesystem->getHandler().tryCompleteMultiple();
      tryToAcknowledge();
    }
    bool found = false;
    if (isSlotReady(m_readerSlot)) {
      if (m_debug) {
        std::cout << "[DBG] CB " + std::to_string(m_id) + " Creating processing task for slot " + std::to_string(m_readerSlot)
                  + " with reader slot " + std::to_string(m_readerSlot) << std::endl;
      }
      m_slots[m_readerSlot].m_slot.store(3);
      index = m_readerSlot * m_batchSize;
      length = m_batchSize;
      m_readerSlot++;
      if (m_readerSlot == getNumberOfSlots()) {
        m_readerSlot = 0;
      }
      found = true;
    }
    return found;
  }

  void free() override {
    throw std::invalid_argument("error: this operator is not supported yet");
  }

  void free(long offset, bool isPersistent = false) override {
    if (SystemConf::getInstance().LINEAGE_ON && !isPersistent)
      return;

    long _start = m_startP.m_value.load(std::memory_order_relaxed);

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

    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " Freeing slot " + std::to_string(m_nextFreeSlot)
                + " with empty slots " + std::to_string(m_emptySlots) << std::endl;
    }

    m_slots[m_nextFreeSlot].m_memcpyFinished.store(false);
    m_slots[m_nextFreeSlot].m_slot.store(-1);
    m_emptySlots.fetch_add(1);

    m_nextFreeSlot++;
    if (m_nextFreeSlot == m_numberOfSlots) m_nextFreeSlot = 0;

    /* Set new start pointer */
    m_startP.m_value.store(_start + bytes, std::memory_order_relaxed);
    m_root->m_bytesProcessed.get_rw().fetch_add(bytes,
                                                std::memory_order_release);
    //m_root->m_startP.get_rw().store(_start + bytes, std::memory_order_release);
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

  void createInsertionTask(char *values, long bytes, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_query, nullptr,
        &m_query->getWindowDefinition(), m_query->getSchema(),
        -1);
    batch->setTaskType(TaskType::INSERT);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, TaskType::INSERT);
    auto slot = getNextSlotToWrite();
    auto slotId = slot % m_numberOfSlots;
    if (slot >= m_numberOfSlots) {
      m_wraps = 1;
    }
    if (SystemConf::getInstance().LINEAGE_ON) {
      if (!graph)
        graph = LineageGraphFactory::getInstance().newInstance();
      m_slots[slotId].setLineageGraph(graph);
      graph.reset();
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized before task creation for slot " + std::to_string(slot));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      m_slots[slotId].m_fptr = fptr;
    }

    auto oldVal = -1;
    while (!m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 0)) {
      std::cout << "warning: adding data to slot " << slotId
                << " is blocked with oldVal " << oldVal << std::endl;
      _mm_pause();
    }

    if (m_debug) {
      std::cout << "slot " << slotId << " is set to " << m_slots[slotId].m_slot.load() << std::endl;
    }
    task->setInsertion(values, bytes, slot, latencyMark, retainMark, m_wraps);
    while (!m_query->getTaskQueue()->try_enqueue(task))
      ;
  }

  void createInsertionTask(std::shared_ptr<UnboundedQueryBuffer> &values, long bytes, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_query, nullptr,
        &m_query->getWindowDefinition(), m_query->getSchema(),
        -1);
    batch->setTaskType(TaskType::INSERT);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, TaskType::INSERT);
    auto slot = getNextSlotToWrite();
    auto slotId = slot % m_numberOfSlots;
    if (slot >= m_numberOfSlots) {
      m_wraps = 1;
    }
    if (SystemConf::getInstance().LINEAGE_ON) {
      if (!graph)
        graph = LineageGraphFactory::getInstance().newInstance();
      m_slots[slotId].setLineageGraph(graph);
      graph.reset();
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized before task creation for slot " + std::to_string(slot));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      m_slots[slotId].m_fptr = fptr;
    }

    auto oldVal = -1;
    while (!m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 0)) {
      std::cout << "warning: adding data to slot " << slotId
                << " is blocked with oldVal " << oldVal << std::endl;
      _mm_pause();
    }

    if (m_debug) {
      std::cout << "slot " << slotId << " is set to " << m_slots[slotId].m_slot.load() << std::endl;
    }
    task->setInsertion(values, bytes, slot, latencyMark, retainMark, m_wraps);
    while (!m_query->getTaskQueue()->try_enqueue(task))
      ;
  }

  void createInsertionTask(void *values, long bytes, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, m_query, nullptr,
        &m_query->getWindowDefinition(), m_query->getSchema(),
        -1);
    batch->setTaskType(TaskType::INSERT);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, TaskType::INSERT);
    auto slot = getNextSlotToWrite();
    auto slotId = slot % m_numberOfSlots;
    if (slot >= m_numberOfSlots) {
      m_wraps = 1;
    }
    if (SystemConf::getInstance().LINEAGE_ON) {
      if (!graph)
        graph = LineageGraphFactory::getInstance().newInstance();
      m_slots[slotId].setLineageGraph(graph);
      graph.reset();
      if (!m_slots[slotId].m_graph)
        throw std::runtime_error("error: the lineage graph is not initialized before task creation for slot " + std::to_string(slot));
      auto bufferId = (m_id % 2 == 0) ? 0 : 1;
      auto fptr = m_fileStore->getFilePtr(m_query->getId(), bufferId, slot * m_batchSize);
      m_slots[slotId].m_fptr = fptr;
    }

    auto oldVal = -1;
    while (!m_slots[slotId].m_slot.compare_exchange_weak(oldVal, 0)) {
      std::cout << "warning: adding data to slot " << slotId
                << " is blocked with oldVal " << oldVal << std::endl;
      _mm_pause();
    }

    if (m_debug) {
      std::cout << "slot " << slotId << " is set to " << m_slots[slotId].m_slot.load() << std::endl;
    }
    task->setInsertion((void *)values, bytes, slot, latencyMark, retainMark, m_wraps);
    while (!m_query->getTaskQueue()->try_enqueue(task))
      ;
  }

  bool isPersistent() override { return true; }

  void updateFileEndPtr(long id) override {
    m_root->m_endId.get_rw().store(id);
  }

  void updateFileStartPtr(long id, long offset) override {
    if (m_debug) {
      std::cout << "[DBG] CB " + std::to_string(m_id) + " garbage collection: updating the start id to " + std::to_string(id)
      + " and the offset to " + std::to_string(offset) + " (prev offset: "
      + std::to_string(m_prevFreeOffset) + ")" << std::endl;
    }
    if (offset < m_prevFreeOffset) {
      //throw std::runtime_error("error: trying to free an invalid offset " +
      //                         std::to_string(offset) + " < " +
      //                         std::to_string(m_prevFreeOffset));
      //std::cout << "warning: trying to free an invalid offset in cqbuffer " +
      //                 std::to_string(offset) + " < " + std::to_string(m_prevFreeOffset) << std::endl;
      return;
    }
    m_prevFreeOffset = offset;

    m_root->m_startId.get_rw().store(id);
    m_root->m_startP.get_rw().store(offset);
  }

  void updateStepAndOffset(long step, long offset) {
    m_root->m_step.get_rw().store(step);
    m_root->m_offset.get_rw().store(offset);
  }

  void getStepAndOffset(long &step, long &offset) {
    step = m_root->m_step.get_ro().load();
    offset = m_root->m_startP.get_ro().load(); //m_root->m_offset.get_ro().load();
  }

  size_t getBytesProcessed() override { return m_root->m_bytesProcessed.get_ro(); }

  size_t getUnsafeStartPointer() override {
    return m_root->m_startP.get_ro().load(std::memory_order_relaxed);
  }

  void incrementUnsafeStartPointer(size_t offset) override {
    auto start = m_root->m_startP.get_ro().load();
    auto bufferId = (m_id % 2 == 0) ? 0 : 1;
    m_fileStore->freePersistent(m_query->getId(), bufferId, start + offset);
  }

  size_t getUnsafeEndPointer() override {
    return m_root->m_endP.get_ro().load(std::memory_order_relaxed);
  }

  size_t getUnsafeRemainingBytes() override {
    auto start = m_root->m_startP.get_ro().load();
    auto end = m_root->m_endP.get_ro().load();
    return end-start;
  }

  bool isSlotReady(size_t slotId) {
    checkSlotNumber(slotId);
    return m_slots[slotId].m_slot.load() == 2 && m_slots[slotId].m_memcpyFinished;
  }

  void checkSlotNumber(size_t slotId) {
    if (slotId >= m_numberOfSlots)
      throw std::invalid_argument("error: slotId >= m_numberOfSlots");
  }

  size_t getSlotId(size_t index) { return std::floor(index / m_batchSize); }

  void clearPersistentMemory() {
    m_pop.close();
    m_filesystem->eraseFiles();
  }

  ~PersistentCircularQueryBuffer() override {
      m_pop.close();
  };

 private:
  void tryToAcknowledge() {
    try {
      if (!m_ackLock.try_lock()) return;

      while (true) {
        m_filesystem->getHandler().tryCompleteMultiple();
        auto slotId = m_nextPersistentSlot.load();
        if (m_slots[slotId].m_slot.load() != 1) {
          break;
        }

        if (m_debug) {
          std::cout << "[DBG] CB " + std::to_string(m_id) + " Acknowledging slot " + std::to_string(slotId) << std::endl;
        }

        // m_endP.fetch_add(_4MB);
        if (m_numberOfSlotsToFree == 0) {
          m_root->m_endP.get_rw().fetch_add(m_batchSize, std::memory_order_release);
        } else {
          if (m_compress) {
            auto index = slotId * m_batchSize;
            int writePos = 0;
            bool copy = true;
            auto latency = (SystemConf::getInstance().LATENCY_ON) ? 0 : -1;
            m_decompressionFP[m_compPos](0, &m_buffer[index], 0, m_batchSize, m_copyBuffers[0].data(), writePos, SystemConf::getInstance().BLOCK_SIZE, copy, latency);
            if (writePos != SystemConf::getInstance().BATCH_SIZE)
              throw std::runtime_error("error: the write position is not equal to the batch size after decompression");
            //std::cout << "[DBG] decompressing for slot " << slotId << " and index " << index << std::endl;
          }
          m_numberOfSlotsToFree--;
        }
        m_endP.m_value.fetch_add(m_batchSize, std::memory_order_relaxed);

        m_nextPersistentSlot.fetch_add(1);
        m_slots[slotId].m_slot.store(2);

        if (m_nextPersistentSlot.load() == m_numberOfSlots)
          m_nextPersistentSlot.store(0);
      }

      m_ackLock.unlock();
    } catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
  }

  struct PMem {
    pmem::obj::p<std::atomic<long>> m_startP;
    pmem::obj::p<std::atomic<long>> m_endP;
    pmem::obj::p<std::atomic<size_t>> m_bytesProcessed;
    pmem::obj::p<std::atomic<long>> m_step;
    pmem::obj::p<std::atomic<long>> m_offset;
    pmem::obj::p<std::atomic<int>> m_startId;
    pmem::obj::p<std::atomic<int>> m_endId;
    pmem::obj::p<std::atomic<long>> m_blockSize;
    pmem::obj::persistent_ptr<PMem> next;
    PMem() {
      m_startP.get_rw() = 0L;
      m_endP.get_rw() = 0L;
      m_bytesProcessed.get_rw() = 0L;
      m_blockSize.get_rw() = 0L;
    };

    void updateBlockSize(long const& value) {
      auto prev_value = m_blockSize.get_ro().load();
      while(prev_value < value &&
          !m_blockSize.get_rw().compare_exchange_weak(prev_value, value))
      {}
    }

    /** Copy constructor is deleted */
    PMem(const PMem &) = delete;
    /** Assignment operator is deleted */
    PMem &operator=(const PMem &) = delete;
  };
};