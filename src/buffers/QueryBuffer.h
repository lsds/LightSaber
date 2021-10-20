#pragma once

#include <atomic>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "RDMABufferPool.h"
#include "checkpoint/BlockManager.h"
#include "checkpoint/LineageGraphFactory.h"
#include "compression/CompressionStatistics.h"
#include "filesystem/File.h"
#include "filesystem/FileSystemDisk.h"
#include "tbb/concurrent_queue.h"
#include "utils/PaddedLong.h"
#include "utils/SystemConf.h"
#include "utils/Utils.h"

class Query;
class UnboundedQueryBuffer;

/*
 * \brief QueryBuffer is used as a base class for implementing a lock-free circular buffer.
 *
 * A single buffer is used per input stream to store incoming tuples and acts as a
 * SPSC queue.
 *
 * */

class QueryBuffer {

 protected:
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

  typedef QueueIoHandler adapter_t;
  typedef FileSystemDisk<adapter_t> disk_t;
  typedef typename FileSystemDisk<adapter_t>::file_t file_t;

  std::shared_ptr<disk_t> m_filesystem = nullptr;

  // used for checkpoints
  size_t m_batchSize;
  Query *m_query;
  bool m_compress = false;
  std::unique_ptr<CompressionStatistics> m_compStats;
  std::vector<std::function<void(int, char *, int, int, char *, int &, int, bool &, long)>> m_compressionFP;
  std::vector<std::function<void(int, char *, int, int, char *, int &, int, bool &, long)>> m_decompressionFP;
  std::atomic<int> m_compPos = 0;
  std::function<void(int, char *, int, uint32_t *&, double *&, double *&, double *&, double *&)> m_instrFP;
  std::atomic<bool> m_startInstr = false;

  long m_prevFreeOffset = -1;

  std::atomic<size_t> m_storedBytes = 0;
  std::atomic<size_t> m_storedCounter = 0;

  // Slots should have a 4MB size for more efficient writes
  struct Slot;
  size_t m_numberOfSlots;
  std::atomic<size_t> m_emptySlots;
  std::vector<Slot> m_slots;
  std::atomic<size_t> m_nextSlotToWrite;
  // Reader Variables
  size_t m_readerSlot = 0;

  long m_previousBlockSize = 0;
  int m_numberOfSlotsToRecover = 0;
  int m_numberOfSlotsToFree = 0;
  BlockManager *m_fileStore;

  // used for RDMA ingestion
  bool m_filter = false;
  std::function<void(char *, int, int, char *, int, int &)> m_filterFP;

 public:
  QueryBuffer(int id, size_t capacity, bool isNuma, int tupleSize = 1,
              bool copyDataOnInsert = true,
              size_t batchSize = SystemConf::getInstance().BATCH_SIZE,
              std::shared_ptr<disk_t> filesystem = nullptr, bool clearFiles = true)
      : m_startP(0L),
        m_endP(0L),
        m_capacity(upper_power_of_two(capacity)),
        m_mask(m_capacity - 1),
        m_wraps(0),
        m_bytesProcessed(0L),
        m_tuplesProcessed(0L),
        m_tasksProcessed(0L),
        m_temp(0),
        m_id(id),
        m_isNuma(isNuma),
        m_tupleSize(tupleSize),
        m_copyDataOnInsert(copyDataOnInsert),
        m_filesystem(filesystem),
        m_batchSize(batchSize),
        m_numberOfSlots(capacity / m_batchSize),
        m_emptySlots(m_numberOfSlots),
        m_slots(2 * m_numberOfSlots),
        m_nextSlotToWrite(0) {
    if (clearFiles) {
      std::vector<std::string> files;
      auto path = SystemConf::FILE_ROOT_PATH + "/scabbard";
      Utils::tryCreateDirectory(path);
      Utils::readDirectory(path, files);
      for (auto &f : files) {
        if (f == ("queue_pm_" + std::to_string(id)) ||
            f == ("queue_data_" + std::to_string(id))) {
          auto res = std::remove((path+"/"+f).c_str());
          if (res != 0)
            std::cout << "Failed to remove file " << (path+"/"+f) << std::endl;
        }
      }
    }
  };

  size_t upper_power_of_two(size_t v) {
    size_t power = 1;
    while (power < v)
      power *= 2;
    return power;
  }

  virtual long put(char *values, long bytes, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) = 0;

  virtual long put(std::shared_ptr<UnboundedQueryBuffer> &values, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) {
    throw std::runtime_error("error: the put function with UnboundedQueryBuffer is not implemented");
  }

  virtual long put(void *values, long latencyMark = -1, long retainMark = -1, std::shared_ptr<LineageGraph> graph = nullptr) {
    throw std::runtime_error("error: the put function with UnboundedQueryBuffer is not implemented");
  }

  virtual void putRows(int pid, char *values, long bytes, size_t slot, long latencyMark = -1, long retainMark = -1, int wraps = 0) {
    throw std::runtime_error("error: the putRows function is not implemented");
  }

  virtual void putRows(int pid, std::shared_ptr<UnboundedQueryBuffer> &values, long bytes, size_t slot, long latencyMark = -1, long retainMark = -1, int wraps = 0) {
    throw std::runtime_error("error: the putRows function with UnboundedQueryBuffer is not implemented");
  }

  virtual void putRows(int pid, void *values, long bytes, size_t slot, long latencyMark = -1, long retainMark = -1, int wraps = 0) {
    throw std::runtime_error("error: the putRows function with UnboundedQueryBuffer is not implemented");
  }

  virtual long recover(int &bytes) {
    throw std::runtime_error("error: the recover function is not implemented");
  }

  virtual void prepareRecovery() {
    throw std::runtime_error("error: the prepareRecovery function is not implemented");
  }

  virtual void free() = 0;

  virtual void free(long offset, bool isPersistent = false) = 0;

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

  virtual char *getBufferRaw() = 0;

  unsigned long getWraps() {
    return m_wraps;
  }

  virtual size_t getMask() {
    return m_mask;
  }

  virtual size_t getCapacity() {
    return m_capacity;
  }

  virtual size_t getBufferCapacity(int id) = 0;

  virtual size_t getBytesProcessed() {
    return m_bytesProcessed.load(std::memory_order_relaxed);
  }

  virtual size_t getTuplesProcessed() {
    return m_tuplesProcessed.load(std::memory_order_relaxed);
  }

  virtual size_t getTasksProcessed() {
    return m_tasksProcessed.load(std::memory_order_relaxed);
  }

  void debug() {
    long head = m_startP.m_value.load(std::memory_order_relaxed);
    long tail = m_endP.m_value.load(std::memory_order_relaxed);
    long remaining = (tail < head) ? (head - tail) : (m_capacity - (tail - head));

    std::cout << "[DBG] start " + std::to_string(head) + " end " + std::to_string(tail) +
        " wraps " + std::to_string(m_wraps) + " " + std::to_string(remaining) << std::endl;
  }

  virtual long getLong(size_t index) = 0;

  virtual void setLong(size_t index, long value) = 0;

  virtual void appendBytesTo(int startPos, int endPos, ByteBuffer &outputBuffer) = 0;

  virtual void appendBytesTo(int startPos, int endPos, char *output) = 0;

  virtual bool getIsNumaWrapper() {
    return m_isNuma;
  }

  virtual bool isPersistent() {
    return false;
  }

  virtual size_t getUnsafeStartPointer() {
    return m_startP.m_value.load(std::memory_order_relaxed);
  }

  virtual void incrementUnsafeStartPointer(size_t offset) {
    m_startP.m_value.fetch_add(offset);
  }

  virtual size_t getUnsafeEndPointer() {
    return m_endP.m_value.load(std::memory_order_relaxed);
  }

  virtual size_t getUnsafeRemainingBytes() {
    return m_endP.m_value.load(std::memory_order_relaxed)-m_startP.m_value.load(std::memory_order_relaxed);
  }

  size_t getAverageStoredBytes() {
    if (m_storedCounter > 0) {
      return (m_storedBytes/m_storedCounter);
    }
    return 0;
  }

  size_t getBatchSize() {
    return m_batchSize;
  }

  void fixTimestamps(size_t index, long timestamp, long step, long batchSize) {
      throw std::runtime_error("error: this function is not implemented");
  }

  void setCompressionFP(std::function<void(int, char *, int, int, char *, int &, int, bool &, long)> fp, size_t compPos = 0) {
    m_compressionFP.push_back(fp);
    m_compPos = compPos;
    m_compress = true;
  }

  void setDecompressionFP(std::function<void(int, char *, int, int, char *, int &, int, bool &, long)> fp) {
    m_decompressionFP.push_back(fp);
  }

  void setFilterFP(std::function<void(char *, int, int, char *, int, int &)> fp) {
    m_filterFP = std::move(fp);
    m_filter = true;
  }

  bool hasCompressionPolicyChanged() {
    if (m_compress && SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON) {
      return m_compStats->updateCompressionDecision();
    }
    return false;
  }

  CompressionStatistics *getCompressionStatistics() {
    if (!m_compress || !SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON || !m_compStats) {
      throw std::runtime_error("error: adaptive compression is not enabled");
    }
    return m_compStats.get();
  }
  bool hasCompression(std::function<void(int, char *, int, int, char *, int &, int, bool &, long)> &fp) {
    if (m_compress) {
      fp = m_compressionFP[m_compPos];
      return true;
    }
    return false;
  }

  void enableInstrumentation(std::function<void(int, char *, int, uint32_t *&, double *&, double *&, double *&, double *&)> fp) {
    m_instrFP = fp;
    m_startInstr = true;
  }

  void setQuery(Query *query, std::vector<ColumnReference *> *cols = nullptr) {
    m_query = query;
    if (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON)
      m_compStats = std::make_unique<CompressionStatistics>(m_id, cols);
  }

  virtual void setupForCheckpoints(std::shared_ptr<disk_t> filesystem) {
    throw std::runtime_error("error: the setupForCheckpoints function is not implemented");
  }

  virtual int prepareCheckpoint(long freePtr, tbb::concurrent_queue<int> &readySlots, int &firstSlot, int &lastSlot) {
    throw std::runtime_error("error: the prepareCheckpoint function is not implemented");
  }

  size_t getNumberOfSlots() { return m_numberOfSlots; }

  size_t getEmptySlots() { return m_emptySlots; }

  std::vector<Slot> &getSlots() { return m_slots; }

  void setNumberOfSlotsToRecover(int slots) {
    m_numberOfSlotsToRecover = slots;
    m_numberOfSlotsToFree = slots;
    std::cout << "[DBG] buffer " + std::to_string(m_id) << " has to recover " +
        std::to_string(slots) + " slots" << std::endl;
  }

  //void setFilesystem(std::shared_ptr<BlockManager> &store) { m_fileStore = store; }

  std::shared_ptr<disk_t> getFilesystem() { return m_filesystem; }

  void setFileStore(BlockManager *store) { m_fileStore = store; }

  BlockManager *getFileStore() { return m_fileStore; }

  virtual void updateFileEndPtr(long id) {
    throw std::runtime_error("error: the updateFileEndPtr function is not implemented");
  }

  virtual void updateFileStartPtr(long id, long offset) {
    throw std::runtime_error("error: the updateFileStartPtr function is not implemented");
  }

  virtual void updateStepAndOffset(long step, long offset) {
    throw std::runtime_error("error: the updateStepAndOffset function is not implemented");
  }

  virtual void getStepAndOffset(long &step, long &offset) {
    throw std::runtime_error("error: the getStepAndOffset function is not implemented");
  }

  int getRemainingSlotsToFree() {
    return m_numberOfSlotsToFree;
  }

  virtual ~QueryBuffer() = default;

 protected:
  void setMask(const size_t mask) {
    m_mask = mask;
  }

  void setCapacity(const size_t capacity) {
    m_capacity = capacity;
  }

  int roundOffset(int offset) {
    if (!m_filesystem)
      throw std::runtime_error("error: the filesystem is not initialized");
    auto alignment = m_filesystem->getSectorSize();
    if (offset < 8 * 1024 && offset != 0) {
      offset = 8 * 1024;
    } else if (offset % alignment != 0) {
      auto d = offset / alignment;
      offset = (d + 1) * alignment;
    }
    return offset;
  }

  size_t getNextSlotToWrite() {
    return m_nextSlotToWrite++;
  }

  struct alignas(64) Slot {
    int m_id = -1;
    size_t m_size; //SystemConf::_4MB;
    std::atomic<int> m_slot;
    std::atomic<bool> m_memcpyFinished;
    std::atomic<int> m_numberOfResults;
    std::atomic<long> m_taskId;
    std::atomic<int> m_previousSlot;
    std::mutex m_updateLock;
    char *m_bufferPtr = nullptr;
    std::shared_ptr<LineageGraph> m_graph = nullptr;
    bool m_ready;
    file_t *m_fptr;

    Slot() : m_slot(-1), m_memcpyFinished(false), m_numberOfResults(0), m_taskId(-1), m_previousSlot(-1), m_ready(false) {}

    void setId(int id, size_t batchSize, char *bufferPtr) {
      m_id = id;
      m_size = batchSize;
      m_bufferPtr = bufferPtr;
    }

    void setLineageGraph(std::shared_ptr<LineageGraph> &graph) {
      //std::lock_guard<std::mutex> l (m_updateLock);
      if (m_graph) {
        if (m_graph.use_count() == 1)
          LineageGraphFactory::getInstance().free(m_graph);
        m_graph.reset();
      }
      m_graph = std::move(graph);
      graph.reset();
    }

    std::shared_ptr<LineageGraph> getLineageGraph() {
      //std::lock_guard<std::mutex> l (m_updateLock);
      auto graph = m_graph;
      m_graph.reset();
      return graph;
    }

    void setNumberOfResults () {
      m_numberOfResults.store(1);
    }

    int getNumberOfResults () {
      m_numberOfResults.store(1);
      return m_numberOfResults.load();
    }

    void setPreviousSlot (int prev) {
      if (prev != 1 && prev != 3) {
        throw std::runtime_error("error: setting the previous slot value to " + std::to_string(prev));
      }
      m_previousSlot = prev;
    }

    int getPreviousSlot () {
      if (m_previousSlot != 1 && m_previousSlot != 3) {
        throw std::runtime_error("error: getting the previous slot value " + std::to_string(m_previousSlot));
      }
      return m_previousSlot.load();
    }

    void prefetch() {
      if (m_bufferPtr)
        __builtin_prefetch(m_bufferPtr, 1, 3);
    }

    void setReady() {
      m_ready = true;
    }

    void reset() {
      m_slot.store(-1);
      m_ready = false;
    }
  };
};