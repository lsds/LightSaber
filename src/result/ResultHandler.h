#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>

#include <boost/circular_buffer.hpp>
#include <list>
#include <mutex>
#include <vector>

#include "utils/PaddedInt.h"
#include "utils/SystemConf.h"

#if defined(RDMA_OUTPUT)
#include "RDMA/infinity/infinity.h"
#include "buffers/RDMABufferPool.h"
#endif

class QueryBuffer;
class PartialWindowResults;
class WindowBatch;
class SystemConf;
class Query;
class AggregateOperatorCode;
struct PartialResultSlotWithoutFragments;
struct PartialResultSlot;
struct PartialWindowResultSlot;
class FileBackedCheckpointCoordinator;
struct LineageGraph;

/*
 * \brief This class handles the result phase of both stateless and stateful
 * operations. The results are re-ordered before pushed to the next pipeline.
 *
 * For stateless operations (@hasWindowFragments is set false), the results are stored in-order based on their task id
 * and forwarded.
 *
 * For the stateful operations (@hasWindowFragments is set true), there are two separate modes:
 * i)  for non-parallel merging a single thread is accessing the state each time and multiple
 *     threads can store their intermediate results in the @PartialResultSlots.
 * ii) for parallel merging, a single thread creates and collects the results of the task
 *     while multiple threads can execute them. As before, multiple threads can store their
 *     intermediate results in the @PartialResultSlots in parallel.
 *
 * At the moment, parallel merge is supported only for sliding windows and grouped aggregations.
 *
 * */

class ResultHandler {
 private:
  Query &m_query;
  QueryBuffer &m_freeBuffer1, &m_freeBuffer2;
  bool m_hasWindowFragments;
  bool m_useParallelMerge;
  std::atomic<int> m_maxTaskId;
  std::mutex m_forwardLock; /* Protects nextToForward */
  std::atomic<int> m_nextToForward;
  std::atomic<int> m_nextWindowToForward;
  std::mutex m_mergeLock;   /* Protects nextToAggregate */
  std::mutex m_prepareMergeLock;   /* Protects nextToAggregate */
  std::atomic<int> m_nextToAggregate;
  AggregateOperatorCode *m_aggrOperator;
  long m_totalOutputBytes;
  int m_numberOfSlots = SystemConf::getInstance().SLOTS;
  int m_numberOfWindowSlots = 2 * SystemConf::getInstance().PARTIAL_WINDOWS * SystemConf::getInstance().WORKER_THREADS;

  /* Variables used for parallel merging */
  std::mutex m_assignLock; /* Protects nextToForward */
  tbb::concurrent_queue<int> m_availableSlots;
  std::atomic<int> m_reservedSlots;
  std::vector<PartialResultSlot*> m_slotsToRelease;
  int m_currentWindowSlot;
  int m_nextToAggregateWindows;
  int m_nextToForwardPtrs;
  std::vector<std::mutex> m_mergeLocks;   /* Protects nextToAggregate */

  int m_insertedWindows = 0;
  int m_forwardedWindows = 0;

  size_t m_forwardId = 0;

  bool m_stopMerging = false;

  bool m_hasRestored = false;

  std::shared_ptr<LineageGraph> m_graph = nullptr;
  std::shared_ptr<LineageGraph> m_checkpointGraph = nullptr;

  /*
   * Flags:
   *  -1: slot is free
   *   0: slot is being populated by a thread
   *   1: slot is occupied, but "unlocked", thus it can be aggregated with its next one"
   *   2: slot is occupied, but "locked", thus it is being processed
   * Extra Aggregation Flags:
   *   3: slot is occupied, but "unlocked", thus it is ready to be forwarded
   *   4: slot is occupied, but "locked", thus the result are being forwarded
   */
  // Structures to hold the actual data
  std::vector<PartialResultSlotWithoutFragments> m_resultsWithoutFrags;
  std::vector<PartialResultSlot> m_results;
  std::vector<PartialWindowResultSlot> m_windowResults;
  boost::circular_buffer<int> m_openingWindowsList;

  // Variables for sending data to a sink over TCP
  int m_sock = 0;
  const bool m_compressOutput = false;
  std::vector<ByteBuffer> m_compressBuffers;

#if defined(RDMA_OUTPUT)
  infinity::core::Context *m_context;
  infinity::queues::QueuePairFactory *m_qpFactory;
  infinity::queues::QueuePair *m_qp;
  infinity::memory::Buffer *m_sendBuffer, *m_receiveBuffer;
#endif

  inline void forwardAndFreeWithoutFrags(WindowBatch *batch);
  inline void aggregateAndForwardAndFree(WindowBatch *batch);
  inline void aggregateWindowsAndForwardAndFree(WindowBatch *batch);

  inline void debugAggregateAndForwardAndFree();
  inline void updateMaximumTaskId(int value);

  friend class FileBackedCheckpointCoordinator;

 public:
  ResultHandler(Query &query, QueryBuffer &freeBuffer1, QueryBuffer &freeBuffer2, bool hasWindowFragments, bool useParallelMerge = false);
  void setupSocket();
  long getTotalOutputBytes();
  void incTotalOutputBytes(int bytes);
  void forwardAndFree(WindowBatch *batch);
  void setAggregateOperator(AggregateOperatorCode *aggrOperator);
  bool containsFragmentedWindows();
  void restorePtrs(int taskId);
  virtual ~ResultHandler();

  // Used only for testing
  std::vector<PartialResultSlotWithoutFragments> &getPartialsWithoutFrags();
  std::vector<PartialResultSlot> &getPartials();
  std::vector<PartialWindowResultSlot> &getWindowPartials();
};