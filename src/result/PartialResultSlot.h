#pragma once

#include <boost/circular_buffer.hpp>

#include "cql/operators/AggregateOperatorCode.h"
#include "buffers/PartialWindowResultsFactory.h"
#include "tasks/WindowBatch.h"

static const bool debug = false;

/*
 * \brief PartialResultSlotWithoutFragments are used for simple operators without window fragments
 * (e.g. projections or filters).
 *
 */

struct alignas(64) PartialResultSlotWithoutFragments {
  std::shared_ptr<PartialWindowResults> m_result;
  std::atomic<int> m_slot;
  int m_freePointer;
  // A query can have more than one downstream queries
  int m_latch;
  long m_latencyMark;
  PartialResultSlotWithoutFragments() : m_slot(-1), m_freePointer(INT_MIN),
                                        m_latch(0), m_latencyMark(-1) {}
};

/*
 * \brief PartialResultSlots are used for operators with window fragments and single-threaded merge phase
 *
 */

struct alignas(64) PartialResultSlot {
  std::shared_ptr<PartialWindowResults> m_closingWindows, m_pendingWindows, m_openingWindows, m_completeWindows;
  std::atomic<int> m_slot;
  int m_index;
  int m_freePointer;
  int m_latch;
  long m_latencyMark;
  PartialResultSlot *m_next;

  PartialResultSlot(int index = -1) : m_closingWindows(nullptr),
                                      m_pendingWindows(nullptr),
                                      m_openingWindows(nullptr),
                                      m_completeWindows(nullptr),
                                      m_slot(-1),
                                      m_index(index),
                                      m_freePointer(INT_MIN),
                                      m_latch(0),
                                      m_latencyMark(-1),
                                      m_next(nullptr) {}

  void connectTo(PartialResultSlot *nextSlot) {
    m_next = nextSlot;
  }

  void init(WindowBatch *batch) {
    m_freePointer = batch->getFreePointer();
    m_latencyMark = batch->getLatencyMark();
    m_latch = 0;
    m_closingWindows = batch->getClosingWindows();
    m_openingWindows = batch->getOpeningWindows();
    m_pendingWindows = batch->getPendingWindows();
    m_completeWindows = batch->getCompleteWindows();
    batch->clear();
  }

  void init(WindowBatch *batch, int window) {
    m_freePointer = batch->getFreePointer();
    m_latencyMark = batch->getLatencyMark();
    m_latch = 0;
    batch->clear();
  }

  void release() {
    if (m_closingWindows != nullptr) {
      PartialWindowResultsFactory::getInstance().free(m_closingWindows->getThreadId(), m_closingWindows);
      m_closingWindows.reset();
    }
    if (m_openingWindows != nullptr) {
      PartialWindowResultsFactory::getInstance().free(m_openingWindows->getThreadId(), m_openingWindows);
      m_openingWindows.reset();
    }
    if (m_pendingWindows != nullptr) {
      PartialWindowResultsFactory::getInstance().free(m_pendingWindows->getThreadId(), m_pendingWindows);
      m_pendingWindows.reset();
    }
    if (m_completeWindows != nullptr) {
      PartialWindowResultsFactory::getInstance().free(m_completeWindows->getThreadId(), m_completeWindows);
      m_completeWindows.reset();
    }
  }

  bool releaseEmptyPartials() {
    if (m_closingWindows != nullptr && m_closingWindows->numberOfWindows() == 0) {
      PartialWindowResultsFactory::getInstance().free(m_closingWindows->getThreadId(), m_closingWindows);
      m_closingWindows.reset();
    }
    if (m_openingWindows != nullptr && m_openingWindows->numberOfWindows() == 0) {
      PartialWindowResultsFactory::getInstance().free(m_openingWindows->getThreadId(), m_openingWindows);
      m_openingWindows.reset();
    }
    if (m_pendingWindows != nullptr && m_pendingWindows->numberOfWindows() == 0) {
      PartialWindowResultsFactory::getInstance().free(m_pendingWindows->getThreadId(), m_pendingWindows);
      m_pendingWindows.reset();
    }
    if (m_completeWindows != nullptr && m_completeWindows->numberOfWindows() == 0) {
      PartialWindowResultsFactory::getInstance().free(m_completeWindows->getThreadId(), m_completeWindows);
      m_completeWindows.reset();
    }
    return true;
  }

  bool tryRelease() {
    int cnt = 0;
    if (m_closingWindows != nullptr) {
      if (m_closingWindows->numberOfWindows() == 0 || m_closingWindows.use_count() == 1) {
        cnt++;
        PartialWindowResultsFactory::getInstance().free(m_closingWindows->getThreadId(), m_closingWindows);
        m_closingWindows.reset();
      } else {
        if (debug)
          std::cout << m_closingWindows.use_count() << " closing windows with still a reference\n";
      }
    } else {
      cnt++;
    }
    if (m_openingWindows != nullptr) {
      if (m_openingWindows->numberOfWindows() == 0 || m_openingWindows.use_count() == 1) {
        cnt++;
        PartialWindowResultsFactory::getInstance().free(m_openingWindows->getThreadId(), m_openingWindows);
        m_openingWindows.reset();
      } else {
        if (debug)
          std::cout << m_openingWindows.use_count() << " opening windows with still a reference\n";
      }
    } else {
      cnt++;
    }
    if (m_pendingWindows != nullptr) {
      if (m_pendingWindows->numberOfWindows() == 0 || m_pendingWindows.use_count() == 1) {
        cnt++;
        PartialWindowResultsFactory::getInstance().free(m_pendingWindows->getThreadId(), m_pendingWindows);
        m_pendingWindows.reset();
      } else {
        if (debug)
          std::cout << m_pendingWindows.use_count() << " returning crap\n";
      }
    } else {
      cnt++;
    }
    if (m_completeWindows != nullptr) {
      m_completeWindows.reset();
    }
    return cnt == 3;
  }

  /*
   * Aggregate this node's opening windows with node p's closing or pending windows. The output of this
   * operation will always produce complete or opening windows - never pending and never closing ones.
   */
  void aggregate(PartialResultSlot *partialSlot, AggregateOperatorCode *aggrOperator) {
    if (m_openingWindows->isEmpty()) { /* Nothing to aggregate */
      if ((!partialSlot->m_closingWindows->isEmpty()) || (!partialSlot->m_pendingWindows->isEmpty())) {
        throw std::runtime_error("error: there are no opening windows but next slot has closing or pending windows");
      }
      m_openingWindows->nullify();
      partialSlot->m_closingWindows->nullify();
      partialSlot->m_pendingWindows->nullify();
      return;
    }
    if (partialSlot->m_closingWindows->isEmpty() && partialSlot->m_pendingWindows->isEmpty()) {
      throw std::runtime_error("error: there are opening windows but next slot has neither closing nor pending windows");
    }
    /*
     * Populate this node's complete windows or p's opening windows.
     * And, nullify this node's opening windows and node p's closing
     * and pending ones.
     */
    if (debug) {
      std::cout << "[DBG] aggregate " << std::to_string(m_openingWindows->getPosition()) << " bytes ("
                << std::to_string(m_openingWindows->numberOfWindows())
                << " opening windows) with " << std::to_string(partialSlot->m_closingWindows->getPosition())
                << " bytes ("
                << std::to_string(partialSlot->m_closingWindows->numberOfWindows()) << " closing windows)" << std::endl;
    }
    auto numOfClosingWindows = partialSlot->m_closingWindows->numberOfWindows();
    auto numOfOpeningWindows = m_openingWindows->numberOfWindows();
    auto numOfPendingWindows = partialSlot->m_pendingWindows->numberOfWindows();
    long pos = 0;
    int tupleSize = 0;
    /* Merge opening and closing windows and store the complete result */
    if (numOfClosingWindows > 0) {
      if (numOfOpeningWindows < numOfClosingWindows)
        throw std::runtime_error("error: closing window partial results are more then the opening ones");

      aggrOperator->aggregatePartials(m_openingWindows,
                                      partialSlot->m_closingWindows,
                                      m_completeWindows,
                                      numOfClosingWindows,
                                      pos,
                                      tupleSize,
                                      true);
      m_completeWindows->setPosition(pos);
      m_completeWindows->incrementCount(numOfClosingWindows);
      partialSlot->m_closingWindows->nullify();
    }

    /* There may be some opening windows left, in which case they are aggregated with node partialSlot's pending one.
     * The result will be stored (prepended) in partialSlot's opening windows */
    auto remainingWindows = numOfOpeningWindows - numOfClosingWindows;
    if (remainingWindows) {
      if (numOfPendingWindows != 1) {
        throw std::runtime_error("error: there are opening windows left but next slot has no pending windows");
      }
      if (debug) {
        std::cout << "[DBG] aggregate " << std::to_string(remainingWindows) << " remaining opening windows with pending"
                  << std::endl;
      }
      int nextOpenWindow = numOfClosingWindows;

      aggrOperator->aggregatePartials(m_openingWindows,
                                      partialSlot->m_pendingWindows,
                                      m_completeWindows,
                                      remainingWindows,
                                      pos,
                                      tupleSize,
                                      false);
      /* Prepend this opening windows (starting from `nextOpenWindow`) to node partialSlot's opening windows.
       * We have to shift the start pointers of partialSlot's opening windows down.
       * There are `count` new windows. The window size equal the hash table size or a single tuple if we don't have group by:
       */
      auto windowSize = (aggrOperator->hasGroupBy()) ? SystemConf::getInstance().HASH_TABLE_SIZE
                                                     : 1; //aggrOperator->getValueLength() + 12;
      partialSlot->m_openingWindows->prepend(m_openingWindows.get(),
                                             nextOpenWindow,
                                             remainingWindows,
                                             windowSize,
                                             tupleSize);
      partialSlot->m_pendingWindows->nullify();
    }
    m_openingWindows->nullify();
  }

  bool isReady() {
    if (m_closingWindows->numberOfWindows() > 0 ||
        m_openingWindows->numberOfWindows() > 0 ||
        m_pendingWindows->numberOfWindows() > 0) {
      /*std::cout << "closingWindows: " << m_closingWindows->numberOfWindows() <<
      " openingWindows: " << m_openingWindows->numberOfWindows() <<
      " pendingWindows:" << m_pendingWindows->numberOfWindows() << std::endl;*/
      return false;
    }
    return true;
  }

  std::string toString() {
    std::string s;
    s.append(std::to_string(m_index));
    s.append(" [");
    s.append(std::to_string(m_openingWindows->numberOfWindows())).append(" ");
    s.append(std::to_string(m_closingWindows->numberOfWindows())).append(" ");
    s.append(std::to_string(m_pendingWindows->numberOfWindows())).append(" ");
    s.append(std::to_string(m_completeWindows->numberOfWindows()));
    s.append("] ");
    s.append("free (" + std::to_string(m_freePointer) + ") ");
    return s;
  }
};

// PartialWindowResultSlots are used for operators with window fragments
// and parallel merge phase
struct PartialWindowResultsWrapper {
  std::shared_ptr<PartialWindowResults> m_partialWindows;
  int m_windowPos;
  bool m_isClosing = false;
  PartialWindowResultsWrapper(std::shared_ptr<PartialWindowResults> partialWindows = nullptr,
                              int pos = -1,
                              bool isClosing = false) :
      m_partialWindows(partialWindows), m_windowPos(pos), m_isClosing(isClosing) {};
};

struct CircularList {
  std::vector<PartialWindowResultsWrapper, tbb::cache_aligned_allocator<PartialWindowResultsWrapper>> m_buffer;
  int m_size;
  int m_readIdx;
  int m_writeIdx;
  int m_elements = 0;
  CircularList(int size = 0) : m_buffer(size, PartialWindowResultsWrapper()), m_size(size) {
    m_readIdx = 0;
    m_writeIdx = size - 1;
  }
  void set_capacity(int size) {
    m_buffer.resize(size, PartialWindowResultsWrapper());
    m_size = size;
    m_readIdx = 0;
    m_writeIdx = size - 1;
  }
  void push_back(std::shared_ptr<PartialWindowResults> partialWindows,
                 int windowPos, bool isClosing = false) {
    if (m_elements == m_size) {
      m_buffer.resize(m_size * 2, PartialWindowResultsWrapper());
      m_size = 2 * m_size;
    }

    m_writeIdx++;
    if (m_writeIdx == (int) m_buffer.size())
      m_writeIdx = 0;

    m_buffer[m_writeIdx].m_partialWindows = partialWindows;
    m_buffer[m_writeIdx].m_windowPos = windowPos;
    m_buffer[m_writeIdx].m_isClosing = isClosing;

    m_elements++;
  }
  PartialWindowResultsWrapper *front() {
    if (m_elements > 0)
      return &m_buffer[m_readIdx];
    else
      //return nullptr;
      throw std::runtime_error("error: empty CircularList");
  }
  void pop_front() {
    m_elements--;
    m_readIdx++;
    if (m_readIdx == (int) m_buffer.size())
      m_readIdx = 0;
  }
  int size() { return m_elements; }
  int capacity() { return m_size; }
};

/*
 * \brief PartialWindowResultSlot are used for operators with window fragments and multi-threaded
 * merge phase
 *
 */

struct alignas(64) PartialWindowResultSlot {
  //tbb::concurrent_queue<std::shared_ptr<PartialWindowResultsWrapper>> partialWindows;
  CircularList m_partialWindows;
  //std::vector<std::shared_ptr<PartialWindowResultsWrapper>> partialWindows;
  std::shared_ptr<PartialWindowResults> m_completeWindows;
  int m_completeWindowsStartPos = 0;
  int m_completeWindow = 0;
  int m_length = 0;
  PartialResultSlot *m_resSlot = nullptr;
  std::atomic<int> m_slot;
  std::atomic<bool> m_finalize;
  std::atomic<bool> m_hasComplete;
  size_t m_windowId;
  int m_index;
  int m_latch;
  long m_latencyMark;
  int m_openingBatchId;
  int m_closingBatchId;
  bool m_isFirstAggregation;
  long m_hashTableSize = 0;

  PartialWindowResultSlot(int index = -1) : m_completeWindows(nullptr),
                                            m_slot(-1),
                                            m_finalize(false),
                                            m_hasComplete(false),
                                            m_windowId(-1),
                                            m_index(index),
                                            m_latch(0),
                                            m_latencyMark(-1),
                                            m_openingBatchId(-1),
                                            m_closingBatchId(-1),
                                            m_isFirstAggregation(false) {}

  void init(AggregateOperatorCode *aggrOperator) {
    m_hashTableSize = aggrOperator->getHashTableSizeAfterCodeGeneration();
  }

  void release(QueryBuffer &freeBuffer) {
    if (m_resSlot) {
      if (m_completeWindows != nullptr) {
        PartialWindowResultsFactory::getInstance().free(m_completeWindows->getThreadId(), m_completeWindows);
      }
      m_resSlot->release();
      //bool success = resSlot->tryRelease();
      //if (!success)
      //    throw std::runtime_error("error: invalid state for PartialResultSlot while freeing");
      /* Free input buffer */
      int fPointer = m_resSlot->m_freePointer;
      if (fPointer != INT_MIN)
        freeBuffer.free(fPointer);
      m_resSlot->m_slot.store(-1);
      m_resSlot = nullptr;
    }
    m_completeWindows.reset();
    m_slot.store(-1);
    m_completeWindowsStartPos = 0;
    m_completeWindow = 0;
    m_length = 0;
    //finalize.store(false);
    //hasComplete.store(false);
  }

  /*
   * Aggregate this node's opening windows with node p's closing or pending windows. The output of this
   * operation will always produce complete or opening windows - never pending and never closing ones.
   */
  void aggregate(AggregateOperatorCode *aggrOperator, int pid) {
    std::shared_ptr<PartialWindowResults> partialWindowResult;
    // first aggregate in place and then continue
    if (m_completeWindows == nullptr ||
        m_completeWindows->numberOfWindows() == 0) {
      m_completeWindows = PartialWindowResultsFactory::getInstance().newInstance(pid,
                                                                                 aggrOperator->getHashTableSizeAfterCodeGeneration(),
                                                                                 2);
      m_completeWindows->getStartPointers()[0] = 0;
      m_completeWindows->getStartPointers()[1] = m_hashTableSize; // store the final result in the first half
      //m_completeWindows->getStartPointers()[2] = hashTableSize * 2; // store intermediate results in the second half
      m_completeWindows->incrementCount(1);
    }
    // aggregate whatever is available
    long pos = 0;
    int tupleSize = 0;
    int window = 0;
    bool isClosing = false;
    while ((partialWindowResult = peekNextWindow(window, isClosing)) != nullptr) {
      if (!isClosing) {
        //aggrOperator->aggregateSinglePartial(m_completeWindows, partialWindowResult, window, pos, tupleSize, false);
      } else {
        // pack results and finalize the window
        //aggrOperator->aggregateSinglePartial(m_completeWindows, partialWindowResult, window, pos, tupleSize, true);
        m_completeWindows->setPosition(pos);
        m_completeWindows->setCount(1);
        this->m_slot.store(3);
        this->setHasComplete();
      }
    }
  }

  void aggregateAll(AggregateOperatorCode *aggrOperator, int pid) {
    // aggregate whatever is available
    int startPos = -1;
    int endPos = -1;
    int tupleSize = 0;
    int window = 0;
    bool isClosing = false;
    auto size = m_partialWindows.size();

    for (int i = 0; i < size; ++i) {
      auto pw = m_partialWindows.front();
      window = pw->m_windowPos;
      m_partialWindows.pop_front();
      if (pw->m_partialWindows == nullptr)
        throw std::runtime_error("error: the next partial window for aggregation is null");
      if (i == 0) {
        // memcpy the first hashtable for simplicity
        aggrOperator->aggregateSinglePartial(m_completeWindows,
                                             m_completeWindow,
                                             m_completeWindowsStartPos,
                                             pw->m_partialWindows,
                                             window,
                                             startPos,
                                             endPos,
                                             tupleSize,
                                             false);
      } else {
        aggrOperator->aggregateSinglePartial(m_completeWindows, m_completeWindow, m_completeWindowsStartPos,
                                             pw->m_partialWindows, window, startPos, endPos, tupleSize, false);
      }
      pw->m_partialWindows.reset();
    }
    // pack results in the end
    aggrOperator->aggregateSinglePartial(m_completeWindows,
                                         m_completeWindow,
                                         m_completeWindowsStartPos,
                                         nullptr,
                                         window,
                                         startPos,
                                         endPos,
                                         tupleSize,
                                         true);
    m_completeWindowsStartPos = startPos;
    m_length = endPos - startPos;
    m_slot.store(3);
  }

  std::shared_ptr<PartialWindowResults> peekNextWindow(int &windowPos, bool &isClosing) {
    std::shared_ptr<PartialWindowResultsWrapper> partial = nullptr;
    std::shared_ptr<PartialWindowResults> nextWindow = nullptr;
    //partialWindows.try_pop(partial);
    partial = nullptr; //partialWindows.front();
    m_partialWindows.pop_front();
    if (partial != nullptr) {
      nextWindow = partial->m_partialWindows;
      isClosing = partial->m_isClosing;
      windowPos = partial->m_windowPos;
      partial.reset();
    }

    return nextWindow;
  }

  void tryReleaseWindow(bool isPending, std::shared_ptr<PartialWindowResults> partialWindowResult) {
    if (!isPending || partialWindowResult.use_count() == 1) {
      PartialWindowResultsFactory::getInstance().free(partialWindowResult->getThreadId(), partialWindowResult);
    }
  }

  void setResultSlot(PartialResultSlot *slot) {
    m_resSlot = slot;
  }

  void setOpeningBatchId(int batchId) {
    m_openingBatchId = batchId;
  }

  void setClosingBatchId(int batchId) {
    m_closingBatchId = batchId;
  }

  void setHasAllBatches() {
    m_finalize.store(true);
  }

  void setHasComplete() {
    m_hasComplete.store(true);
  }

  bool hasAllBatches() {
    //if (openingBatchId != -1 && closingBatchId != -1)
    //    return counter.load() == (closingBatchId-openingBatchId);
    //return false;
    return m_finalize.load();
  }

  bool hasWork() {
    //if (partialWindows.unsafe_size() == 0)
    if (m_partialWindows.size() == 0)
      return false;
    return true;
  }

  void packResult(int tupleSize) {
    size_t currentIndex;
    size_t indexToInsert = 0;
    auto completeWindow = m_completeWindows->getBufferPtrs()[0];
    for (currentIndex = 0; currentIndex < (size_t) m_hashTableSize * tupleSize; currentIndex += tupleSize) {
      if (completeWindow[currentIndex] == 1) {
        if (indexToInsert != currentIndex) {
          std::memcpy(&completeWindow[indexToInsert], &completeWindow[currentIndex], tupleSize);
        }
        indexToInsert += tupleSize;
      }
    }
    m_completeWindows->setPosition(indexToInsert);
  }

  bool isReady() {
    return m_hasComplete.load();
  }

  std::string toString() {
    std::string s;
    s.append(std::to_string(m_index));
    s.append(" [");
    //s.append(std::to_string(partialWindows.unsafe_size())).append(" ");
    s.append(std::to_string(m_partialWindows.size())).append(" ");
    s.append(std::to_string(m_completeWindows->numberOfWindows()));
    s.append("] ");
    return s;
  }
};