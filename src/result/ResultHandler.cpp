#include "result/ResultHandler.h"

#include <xmmintrin.h>

#include <algorithm>
#include <stdexcept>

#include "buffers/PartialWindowResultsFactory.h"
#include "buffers/QueryBuffer.h"
#include "dispatcher/TaskDispatcher.h"
#include "monitors/LatencyMonitor.h"
#include "result/PartialResultSlot.h"
#include "tasks/WindowBatch.h"
#include "utils/Query.h"
#include "utils/Utils.h"

ResultHandler::ResultHandler(Query &query, QueryBuffer &freeBuffer,
                             bool hasWindowFragments, bool useParallelMerge)
    : m_query(query),
      m_freeBuffer(freeBuffer),
      m_hasWindowFragments(hasWindowFragments),
      m_useParallelMerge(useParallelMerge),
      m_nextToForward(0),
      m_nextWindowToForward(0),
      m_nextToAggregate(0),
      m_totalOutputBytes(0L),
      m_numberOfSlots(SystemConf::getInstance().SLOTS),
      m_reservedSlots(0),
      m_currentWindowSlot(0),
      m_nextToAggregateWindows(0),
      m_nextToForwardPtrs(0),
      m_mergeLocks(useParallelMerge ? m_numberOfWindowSlots : 0),
      m_resultsWithoutFrags(hasWindowFragments ? 0 : m_numberOfSlots),
      m_results(hasWindowFragments ? m_numberOfSlots : 0),
      m_windowResults(useParallelMerge ? m_numberOfWindowSlots : 0),
      m_openingWindowsList(m_windowResults.size()) {
  if (hasWindowFragments) {
    for (int i = 0, j = i - 1; i < m_numberOfSlots; i++, j++) {
      m_results[i].m_index = i;
      if (j >= 0) m_results[j].connectTo(&m_results[i]);
    }
    m_results[m_numberOfSlots - 1].connectTo(&m_results[0]);
  } else {
    for (int i = 0, j = i - 1; i < m_numberOfSlots; i++, j++) {
      m_resultsWithoutFrags[i].m_index = i;
    }
  }
}

long ResultHandler::getTotalOutputBytes() { return m_totalOutputBytes; }

void ResultHandler::incTotalOutputBytes(int bytes) {
  m_totalOutputBytes += ((long)bytes);
}

void ResultHandler::forwardAndFree(WindowBatch *batch) {
  if (m_hasWindowFragments) {
    if (m_useParallelMerge) {
      aggregateWindowsAndForwardAndFree(batch);
    } else {
      aggregateAndForwardAndFree(batch);
    }
  } else {
    forwardAndFreeWithoutFrags(batch);
  }
}

void ResultHandler::forwardAndFreeWithoutFrags(WindowBatch *batch) {
  int taskId = batch->getTaskId();
  auto currentQuery = batch->getQuery();
  auto result = batch->getOutputBuffer();
  int freePtr = batch->getFreePointer();
  auto latencyMark = batch->getLatencyMark();
  auto taskType = batch->getTaskType();

  if (taskId < 0) { /* Invalid task id */
    return;
  }

  int idx = ((taskId - 1) % m_numberOfSlots);
  try {
    auto oldVal = -1;
    if (taskType == TaskType::PROCESS) {
      while (
          !m_resultsWithoutFrags[idx].m_slot.compare_exchange_weak(oldVal, 0)) {
        std::cout << "warning: result collector (" << std::this_thread::get_id()
                  << ") blocked: query " +
                         std::to_string(currentQuery->getId()) + " task " +
                         std::to_string(taskId) + " slot " + std::to_string(idx)
                  << std::endl;
        _mm_pause();
      }

      m_resultsWithoutFrags[idx].m_freePointer = freePtr;
      m_resultsWithoutFrags[idx].m_result = result;
      m_resultsWithoutFrags[idx].m_latch = 0;
      m_resultsWithoutFrags[idx].m_latencyMark = latencyMark;
      m_resultsWithoutFrags[idx].m_taskId = taskId;

      /* No other thread can modify this slot. */
      m_resultsWithoutFrags[idx].m_slot.store(1);
    }

    /* Forward and free */
    if (!m_forwardLock.try_lock()) return;

    /* No other thread can enter this section */
    /* Is slot `next` occupied? */
    oldVal = 1;
    if (!m_resultsWithoutFrags[m_nextToForward].m_slot.compare_exchange_strong(
            oldVal, 2)) {
      m_forwardLock.unlock();
      return;
    }

    bool busy = true;
    while (busy) {
      auto buffer = m_resultsWithoutFrags[m_nextToForward].m_result;
      int length = buffer->getPosition();

      /* Forward results */
      if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
        /* Forward the latency mark downstream... */
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_resultsWithoutFrags[m_nextToForward].m_latencyMark != -1)) {
          long t1 =
              m_resultsWithoutFrags[m_nextToForward]
                  .m_latencyMark;  //(long) Utils::getSystemTimestamp
                                   //(freeBuffer.getLong
                                   //(resultsWithoutFrags[m_nextToForward].latencyMark));
          long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
          buffer->putLong(0, Utils::pack(t1, t2));
        }

        int nextQuery = m_resultsWithoutFrags[m_nextToForward].m_latch;
        for (int q = nextQuery;
             q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
          if (currentQuery->getDownstreamQuery(q) != nullptr) {
            bool success =
                currentQuery->getDownstreamQuery(q)
                    ->getTaskDispatcher()
                    ->tryDispatch(
                        buffer->getBuffer().data(), length,
                        m_resultsWithoutFrags[m_nextToForward].m_latencyMark);
            if (!success) {
              std::cout << "[DBG] failed to forward results to next query..."
                        << std::endl;
              m_resultsWithoutFrags[m_nextToForward].m_latch = q;
              m_resultsWithoutFrags[m_nextToForward].m_slot.store(1);
              m_forwardLock.unlock();
              return;
            }
          }
        }
      }

      /* Forward to the distributed API */
      /* Measure latency */
      if (currentQuery->isMostDownstream()) {
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_resultsWithoutFrags[m_nextToForward].m_latencyMark != -1)) {
          m_query.getLatencyMonitor().monitor(
              m_freeBuffer,
              m_resultsWithoutFrags[m_nextToForward].m_latencyMark);
        }
      }

      /*
       * Before releasing the result buffer, increment bytes generated. It is
       * important all operators set the position of the buffer accordingly.
       * Assume that the start position is 0.
       */
      incTotalOutputBytes(length);
      buffer->clear();  // reset position
      PartialWindowResultsFactory::getInstance().free(buffer->getThreadId(),
                                                      buffer);

      /* Free input buffer */
      auto fPointer = m_resultsWithoutFrags[m_nextToForward].m_freePointer;
      if (fPointer != INT_MIN) m_freeBuffer.free(fPointer);

      /* Release the current slot */
      m_resultsWithoutFrags[m_nextToForward].m_slot.store(-1);

      /* Increment next */
      m_nextToForward += 1;
      if (m_nextToForward == m_numberOfSlots) m_nextToForward = 0;

      /* Check if next is ready to be pushed */
      oldVal = 1;
      if (!m_resultsWithoutFrags[m_nextToForward]
               .m_slot.compare_exchange_strong(oldVal, 2)) {
        busy = false;
      }
    }
    /* Thread exit critical section */
    m_forwardLock.unlock();

  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

void ResultHandler::aggregateAndForwardAndFree(WindowBatch *batch) {
  int taskId = batch->getTaskId();
  auto currentQuery = batch->getQuery();
  auto taskType = batch->getTaskType();

  if (taskId < 0) { /* Invalid task id */
    return;
  }

  int idx = ((taskId - 1) % m_numberOfSlots);
  try {
    // First try to merge partial results and then if we have complete result
    // try to forward them
    auto oldVal = -1;
    if (taskType == TaskType::PROCESS) {
      while (!m_results[idx].m_slot.compare_exchange_weak(oldVal, 0)) {
        std::cout << "warning: result collector (" << std::this_thread::get_id()
                  << ") blocked: query " +
                         std::to_string(currentQuery->getId()) + " task " +
                         std::to_string(taskId) + " m_slot " +
                         std::to_string(idx)
                  << std::endl;
        _mm_pause();
      }

      /* Slot `idx` has been reserved for this task id */
      m_results[idx].init(batch);
      /* No other thread can modify this slot. */
      m_results[idx].m_slot.store(1);
    }

    /* Try aggregate slots pair-wise */
    if (m_mergeLock.try_lock()) {
      PartialResultSlot *currentSlot;
      PartialResultSlot *nextSlot;
      while (true) {
        currentSlot = &m_results[m_nextToAggregate];
        nextSlot = currentSlot->m_next;

        int currentSlotFlag = currentSlot->m_slot.load();
        if (currentSlotFlag > 1)
          throw std::runtime_error(
              "error: inconsistent state in next result slot to aggregate");
        if (currentSlotFlag < 1) break;
        int nextSlotFlag = nextSlot->m_slot.load();
        if (nextSlotFlag > 1)
          throw std::runtime_error(
              "error: inconsistent state in next result slot to aggregate");
        if (nextSlotFlag < 1) break;

        /* Both currentSlot and nextSlot nodes are ready to aggregate. */
        currentSlot->aggregate(nextSlot, m_aggrOperator);
        if (!currentSlot->isReady())
          throw std::runtime_error(
              "error: result slot aggregated but is not ready");

        m_nextToAggregate = nextSlot->m_index;
        currentSlot->m_slot.store(3);  // READY for forwarding
        if (nextSlot->isReady()) {
          m_nextToAggregate = nextSlot->m_next->m_index;
          nextSlot->m_slot.store(3);  // READY for forwarding
        }
      }
      m_mergeLock.unlock();
    }

    /* Forward and free */
    if (!m_forwardLock.try_lock()) return;

    /* No other thread can enter this section */
    /* Is slot `next` occupied? */
    oldVal = 3;
    if (!m_results[m_nextToForward].m_slot.compare_exchange_strong(oldVal, 4)) {
      m_forwardLock.unlock();
      return;
    }

    bool busy = true;
    while (busy) {
      auto buffer = m_results[m_nextToForward].m_completeWindows;
      int length = buffer->getPosition();

      /* Forward results */
      if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
        /* Forward the latency mark downstream... */
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_results[m_nextToForward].m_latencyMark != -1)) {
          long t1 =
              m_results[m_nextToForward]
                  .m_latencyMark;  //(long) Utils::getSystemTimestamp
                                   //(freeBuffer.getLong
                                   //(results[m_nextToForward].latencyMark));
          long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
          buffer->putLong(0, Utils::pack(t1, t2));
        }

        int nextQuery = m_results[m_nextToForward].m_latch;
        for (int q = nextQuery;
             q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
          if (currentQuery->getDownstreamQuery(q) != nullptr) {
            bool success =
                currentQuery->getDownstreamQuery(q)
                    ->getTaskDispatcher()
                    ->tryDispatch(buffer->getBuffer().data(), length,
                                  m_results[m_nextToForward].m_latencyMark);
            if (!success) {
              std::cout << "[DBG] failed to forward results to next query..."
                        << std::endl;
              m_results[m_nextToForward].m_latch = q;
              m_results[m_nextToForward].m_slot.store(3);
              m_forwardLock.unlock();
              return;
            }
          }
        }
      }

      /* Forward to the distributed API */
      /* Measure latency */
      if (currentQuery->isMostDownstream()) {
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_results[m_nextToForward].m_latencyMark != -1)) {
          m_query.getLatencyMonitor().monitor(
              m_freeBuffer, m_results[m_nextToForward].m_latencyMark);
        }
      }

      /*
       * Before releasing the result buffer, increment bytes generated. It is
       * important all operators set the position of the buffer accordingly.
       * Assume that the start position is 0.
       */
      incTotalOutputBytes(length);

      /* Free input buffer */
      auto fPointer = m_results[m_nextToForward].m_freePointer;
      if (fPointer != INT_MIN) m_freeBuffer.free(fPointer);

      m_results[m_nextToForward].release();
      /* Release the current slot */
      m_results[m_nextToForward].m_slot.store(-1);

      /* Increment next */
      m_nextToForward += 1;
      if (m_nextToForward == m_numberOfSlots) m_nextToForward = 0;

      /* Check if next is ready to be pushed */
      oldVal = 3;
      if (!m_results[m_nextToForward].m_slot.compare_exchange_strong(oldVal,
                                                                     4)) {
        busy = false;
      }
    }
    /* Thread exit critical section */
    m_forwardLock.unlock();

  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

/*
 * Flags:
 *  -1: slot is free
 *   0: slot is being populated by a thread
 *   1: slot is occupied, but "unlocked", thus it can be aggregated with its
 * next one" 2: slot is occupied, but "locked", thus it is being processed Extra
 * Aggregation Flags: 3: slot is occupied with BufferPtrs result, but
 * "unlocked", thus it is ready to be forwarded 4: slot is occupied with Buffer
 * result, but "unlocked", thus it is ready to be forwarded 5: slot is occupied,
 * but "locked", thus the result are being forwarded
 */
void ResultHandler::aggregateWindowsAndForwardAndFree(WindowBatch *batch) {
  int pid = batch->getPid();
  int taskId = batch->getTaskId();
  auto currentQuery = batch->getQuery();
  auto taskType = batch->getTaskType();

  if (taskId < 0) { /* Invalid task id */
    return;
  }

  int taskIdx = ((taskId - 1) % m_numberOfSlots);
  try {
    // First try to merge partial results and then if we have complete result
    // try to forward them
    auto oldVal = -1;
    if (taskType == TaskType::PROCESS) {
      while (!m_results[taskIdx].m_slot.compare_exchange_weak(oldVal, 0)) {
        std::cout << "warning: result collector (" << std::this_thread::get_id()
                  << ") blocked: query " +
                         std::to_string(currentQuery->getId()) + " task " +
                         std::to_string(taskId) + " slot " +
                         std::to_string(taskIdx)
                  << std::endl;
        _mm_pause();
      }
      if (batch->getCompleteWindows() == nullptr ||
          batch->getClosingWindows() == nullptr ||
          batch->getPendingWindows() == nullptr ||
          batch->getOpeningWindows() == nullptr)
        throw std::runtime_error("error: a partial window is nullptr");

      /* Slot `idx` has been reserved for this task id */
      m_results[taskIdx].init(batch);
      /* No other thread can modify this slot. */
      m_results[taskIdx].m_slot.store(1);
    }

    /* Try assign window ids to partials in slots pair-wise */
    if (m_prepareMergeLock.try_lock()) {
      PartialResultSlot *currentSlot;
      PartialResultSlot *nextSlot;
      while (true) {
        currentSlot = &m_results[m_nextToAggregate];
        nextSlot = currentSlot->m_next;

        int currentSlotFlag = currentSlot->m_slot.load();
        int windowsToStore = 0;
        windowsToStore += (currentSlot->m_openingWindows)
                              ? currentSlot->m_openingWindows->numberOfWindows()
                              : 0;
        windowsToStore +=
            (currentSlot->m_completeWindows)
                ? currentSlot->m_completeWindows->numberOfWindows()
                : 0;
        // if (currentSlotFlag > 1)
        //  throw std::runtime_error("error: invalid slot state equal to " +
        //  std::to_string(currentSlotFlag));
        if ((currentSlotFlag != 1 && currentSlotFlag != 5) ||
            (windowsToStore > m_numberOfWindowSlots - m_reservedSlots.load())) {
          /*std::cout << "[DBG] waiting for empty window slots " +
                           std::to_string(windowsToStore) + " > " +
                           std::to_string(m_numberOfWindowSlots -
                                          m_reservedSlots.load())
                    << std::endl;*/
          break;
        }

        if (debug) {
          std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                    << " starts the window assignment with: "
                    << m_numberOfWindowSlots << " numberOfWindowSlots "
                    << m_nextToAggregate << " nextToAggregate "
                    << m_openingWindowsList.size()
                    << " remaining opening windows " << m_currentWindowSlot
                    << " currentWindowSlot "
                    << currentSlot->m_closingWindows->numberOfWindows()
                    << " closingWindows "
                    << currentSlot->m_pendingWindows->numberOfWindows()
                    << " pendingWindows "
                    << currentSlot->m_completeWindows->numberOfWindows()
                    << " completeWindows "
                    << currentSlot->m_openingWindows->numberOfWindows()
                    << " openingWindows " << std::endl;
        }

#if defined(PREFETCH)
        // prefetch data here
        currentSlot->prefetch();
        nextSlot->prefetch();
#endif
        // try to insert all results to the right slots
        int window, newWindows = 0, latestWindowSlot = -1;
        {
          // first insert closing windows if any
          // when we reach a closing window, the window slot is ready to output
          // a full result
          auto &closingWindows = currentSlot->m_closingWindows;
          if (closingWindows->numberOfWindows() > 0) {
            for (window = 0; window < closingWindows->numberOfWindows();
                 ++window) {
              if (m_openingWindowsList.empty()) {
                std::cout << "[DBG] ForwardAndFree: warning found additional "
                          << "closing window with current window slot "
                          << m_currentWindowSlot << std::endl;
                break;
              }
              auto firstOpeningWindow = m_openingWindowsList.front();
              latestWindowSlot = firstOpeningWindow;
              // while (!(m_windowResults[firstOpeningWindow].slot.load() == 0
              // ||
              //        m_windowResults[firstOpeningWindow].slot.load() == 1))
              while (m_windowResults[firstOpeningWindow].m_slot.load() != 0)
                std::cout << "[DBG] ForwardAndFree [Warning]: closingWindow "
                             "waiting for slot "
                          << m_currentWindowSlot
                          << std::endl;  // partial results are already in
              m_openingWindowsList.pop_front();

              m_windowResults[firstOpeningWindow].m_partialWindows.push_back(
                  // push(
                  // std::make_shared<PartialWindowResultsWrapper>(closingWindows,
                  // window, true));
                  closingWindows, window, true);
              // m_windowResults[firstOpeningWindow].slot.store(2);
              // m_windowResults[firstOpeningWindow].setHasAllBatches();

              // let workers know that there is work
              m_availableSlots.push(firstOpeningWindow);
            }
            // closingWindows.reset();
          }

          // insert complete windows if any, which are ready for forwarding
          auto &completeWindows = currentSlot->m_completeWindows;
          if (completeWindows->numberOfWindows() > 0) {
            latestWindowSlot = m_currentWindowSlot;
            while (m_windowResults[m_currentWindowSlot].m_slot.load() != -1)
              std::cout << "[DBG] ForwardAndFree [Warning]: completeWindow "
                           "waiting for slot "
                        << m_currentWindowSlot
                        << std::endl;  // the slot must be empty
            m_windowResults[m_currentWindowSlot].m_completeWindows =
                completeWindows;
            // set that that the slot is ready to forward with Buffer result
            m_windowResults[m_currentWindowSlot].m_slot.store(3);
            m_windowResults[m_currentWindowSlot].m_completeWindowsStartPos = 0;
            m_windowResults[m_currentWindowSlot].m_length =
                completeWindows->getPosition();
            // m_windowResults[m_currentWindowSlot].setHasComplete();
            m_currentWindowSlot++;
            if (m_currentWindowSlot == m_numberOfWindowSlots)
              m_currentWindowSlot = 0;
            newWindows++;
          }

          // insert pending windows if any
          auto &pendingWindows = currentSlot->m_pendingWindows;
          if (pendingWindows->numberOfWindows() > 0) {
            // auto pendingWindow =
            // std::make_shared<PartialWindowResultsWrapper>(pendingWindows, 0,
            // false); for (window = 0; window <
            // pendingWindows->numberOfWindows(); ++window) {
            for (auto &firstOpeningWindow : m_openingWindowsList) {
              latestWindowSlot = firstOpeningWindow;
              while (m_windowResults[firstOpeningWindow].m_slot.load() != 0)
                std::cout << "[DBG] ForwardAndFree [Warning]: pendingWindow "
                             "waiting for slot "
                          << m_currentWindowSlot
                          << std::endl;  // partial results are already in
              m_windowResults[firstOpeningWindow].m_partialWindows.push_back(
                  // push(pendingWindow);
                  pendingWindows, 0, false);
              // m_windowResults[firstOpeningWindow].slot.store(1);
            }
            // pendingWindows.reset();
          }

          // finally insert opening windows if they exist
          // and keep the earliest window
          auto &openingWindows = currentSlot->m_openingWindows;
          if (openingWindows->numberOfWindows() > 0) {
            for (window = 0; window < openingWindows->numberOfWindows();
                 ++window) {
              latestWindowSlot = m_currentWindowSlot;
              while (m_windowResults[m_currentWindowSlot].m_slot.load() != -1)
                std::cout << "[DBG] ForwardAndFree [Warning]: openingWindow "
                             "waiting for slot "
                          << m_currentWindowSlot
                          << std::endl;  // the slot must be empty
              m_openingWindowsList.push_back(m_currentWindowSlot);

              if (m_windowResults[m_currentWindowSlot]
                      .m_partialWindows.size() == 0)
                m_windowResults[m_currentWindowSlot]
                    .m_partialWindows.set_capacity(4);

              m_windowResults[m_currentWindowSlot].m_partialWindows.push_back(
                  // push(
                  // std::make_shared<PartialWindowResultsWrapper>(openingWindows,
                  // window, false));
                  openingWindows, window, false);

              // reuse the completeWindowsBuffer here
              m_windowResults[m_currentWindowSlot].m_completeWindows =
                  currentSlot->m_completeWindows;
              m_windowResults[m_currentWindowSlot].m_completeWindowsStartPos =
                  currentSlot->m_completeWindows->getPosition();
              m_windowResults[m_currentWindowSlot].m_completeWindow =
                  window;  // currentSlot->completeWindows->numberOfWindows() +
                           // window;

              // set latency mark
              m_windowResults[m_currentWindowSlot].m_latencyMark =
                  currentSlot->m_latencyMark;

              m_windowResults[m_currentWindowSlot].m_slot.store(0);
              m_currentWindowSlot++;
              if (m_currentWindowSlot == m_numberOfWindowSlots)
                m_currentWindowSlot = 0;
              newWindows++;
            }
            // openingWindows.reset();
          }

          m_insertedWindows += newWindows;
          m_reservedSlots.fetch_add(newWindows);

          // sanity check
          /*if (currentSlot->completeWindows->numberOfWindows() > 0 &&
          currentSlot->openingWindows->numberOfWindows() + 2 !=
          currentSlot->completeWindows.use_count()) { std::string errorMsg =
          "error: wrong ref counters after assigning window ids for
          completeWindows " +
                      std::to_string(currentSlot->openingWindows->numberOfWindows())
          + " " + std::to_string(currentSlot->completeWindows.use_count());
              throw std::runtime_error(errorMsg);
          }
          if (currentSlot->openingWindows->numberOfWindows() > 0 &&
              currentSlot->openingWindows->numberOfWindows() + 1 !=
          currentSlot->openingWindows.use_count()) { std::string errorMsg =
          "error: wrong ref counters after assigning window ids for
          openingWindows " +
                                     std::to_string(currentSlot->openingWindows->numberOfWindows())
          + " " + std::to_string(currentSlot->openingWindows.use_count()); throw
          std::runtime_error(errorMsg);
          }
          if (currentSlot->closingWindows->numberOfWindows() > 0 &&
              currentSlot->closingWindows->numberOfWindows() + 1 !=
          currentSlot->closingWindows.use_count()) { std::string errorMsg =
          "error: wrong ref counters after assigning window ids for
          closingWindows " +
                                     std::to_string(currentSlot->closingWindows->numberOfWindows())
          + " " + std::to_string(currentSlot->closingWindows.use_count()); throw
          std::runtime_error(errorMsg);
          }
          if (currentSlot->pendingWindows->numberOfWindows() > 0 &&
              currentSlot->pendingWindows->numberOfWindows() + 1 !=
          currentSlot->pendingWindows.use_count()) { std::string errorMsg =
          "error: wrong ref counters after assigning window ids for
          pendingWindows " +
                                     std::to_string(currentSlot->pendingWindows->numberOfWindows())
          + " " + std::to_string(currentSlot->pendingWindows.use_count()); throw
          std::runtime_error(errorMsg);
          }*/

          // set the end pointer on the result slot in order to free them
          if (latestWindowSlot == -1)
            throw std::runtime_error("error: invalid latestWindowSlot");
          m_windowResults[latestWindowSlot].setResultSlot(currentSlot);

          // Free complete windows here!!!
          currentSlot->m_completeWindows.reset();

          /* Free input buffer */
          // int fPointer = currentSlot->freePointer;
          // if (fPointer != INT_MIN)
          //    freeBuffer.free(fPointer);
        }

        m_nextToAggregate = nextSlot->m_index;
        if (debug) {
          std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                    << " ends the window assignment with: " << m_insertedWindows
                    << " insertedWindows " << m_nextToAggregate
                    << " nextToAggregate " << m_openingWindowsList.size()
                    << " remaining opening windows " << m_currentWindowSlot
                    << " currentWindowSlot " << std::endl;
        }
      }
      m_prepareMergeLock.unlock();
    }

    // try to merge results
    int nextWindowSlotForAggregation;
    bool hasWork = m_availableSlots.try_pop(nextWindowSlotForAggregation);
    while (hasWork) {
      int idx = nextWindowSlotForAggregation;
      if (debug) {
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                  << " start aggregating: " << nextWindowSlotForAggregation
                  << " nextToAggregateWindows " << idx
                  << " m_currentWindowSlot " << std::endl;
      }
#if defined(PREFETCH)
      // prefetch data here
      m_windowResults[idx].prefetch();
#endif
      m_windowResults[idx].aggregateAll(m_aggrOperator, pid);

      // set that that the slot is ready to forward with BufferPtrs
      // if (m_windowResults[idx].isReady()) {
      //    m_windowResults[idx].slot.store(3);
      //}

      if (debug) {
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                  << " finished aggregating: " << nextWindowSlotForAggregation
                  << " nextToAggregateWindows " << idx
                  << " m_currentWindowSlot " << std::endl;
      }
      hasWork = m_availableSlots.try_pop(nextWindowSlotForAggregation);
    }

    // try to forward from m_nextToForward: there are two types to forward
    /* Forward and free */
    if (!m_forwardLock.try_lock()) return;

    /* Release any previous kept slots */
    auto iter = m_slotsToRelease.begin();
    while (iter != m_slotsToRelease.end()) {
      // std::cout << "[DBG] warning: releasing slots after checkpoint" <<
      // std::endl;
      auto slot = *iter;
      if (slot->m_slot.load() == 3) {
        // std::cout << "[DBG] warning: slot " + std::to_string(slot->m_index) +
        // " released " << std::endl;

        slot->release();
        // Free input buffer
        auto fPointer = slot->m_freePointer;
        if (fPointer != INT_MIN) m_freeBuffer.free(fPointer);

        // std::cout << "[DBG] setting slot " + std::to_string(slot->m_index) +
        // " to -1" << std::endl;
        slot->m_slot.store(-1);
        iter = m_slotsToRelease.erase(iter);
        m_nextToForward = (m_nextToForward + 1) % m_numberOfSlots;
      } else {
        break;
      }
    }

    /* No other thread can enter this section */
    /* Is slot `next` occupied? */
    // keep previous value in case we can't forward
    oldVal = m_windowResults[m_nextWindowToForward].m_slot.load();
    if (oldVal < 3) {
      m_forwardLock.unlock();
      return;
    }

    bool busy = true;
    while (busy) {
      if (debug) {
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                  << " start forwarding: " << m_nextWindowToForward
                  << " nextWindowToForward " << std::endl;
      }

      auto &buffer = m_windowResults[m_nextWindowToForward].m_completeWindows;
      int startPos =
          m_windowResults[m_nextWindowToForward].m_completeWindowsStartPos;
      int length = m_windowResults[m_nextWindowToForward].m_length;
      int type = buffer->getType();

      // Forward m_windowResults
      if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
        // Forward the latency mark downstream...
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_windowResults[m_nextWindowToForward].m_latencyMark != -1)) {
          if (type == 1)
            throw std::runtime_error(
                "Latency is not supported for type 1 buffer yet.");
          long t1 =
              m_windowResults[m_nextWindowToForward]
                  .m_latencyMark;  //(long) Utils::getSystemTimestamp
                                   //(freeBuffer.getLong
                                   //(m_windowResults[m_nextWindowToForward].latencyMark));
          long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
          buffer->putLong(0, Utils::pack(t1, t2));
        }

        int nextQuery = m_windowResults[m_nextWindowToForward].m_latch;
        for (int q = nextQuery;
             q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
          if (currentQuery->getDownstreamQuery(q) != nullptr) {
            bool success;
            if (type == 0) {
              success =
                  currentQuery->getDownstreamQuery(q)
                      ->getTaskDispatcher()
                      ->tryDispatch(
                          buffer->getBufferRaw() + startPos, length,
                          m_windowResults[m_nextWindowToForward].m_latencyMark);
            } else {
              // success =
              // currentQuery->getDownstreamQuery(q)->getTaskDispatcher()->tryDispatch(buffer->getBufferPtrs()[0],
              // length);
              throw std::runtime_error(
                  "Forwarding for type 1 buffer is not supported yet.");
            }
            if (!success) {
              std::cout << "[DBG] WindowsForwardAndFree: failed to forward "
                           "results from query "
                        << std::to_string(currentQuery->getId())
                        << " to next query "
                        << std::to_string(
                               currentQuery->getDownstreamQuery(q)->getId())
                        << "..." << std::endl;
              m_windowResults[m_nextWindowToForward].m_latch = q;
              m_windowResults[m_nextWindowToForward].m_slot.store(oldVal);
              m_forwardLock.unlock();
              return;
            }
          }
        }
      }

      /* Forward to the distributed API */
      /* Measure latency */
      if (currentQuery->isMostDownstream()) {
        if (SystemConf::getInstance().LATENCY_ON &&
            (m_windowResults[m_nextWindowToForward].m_latencyMark != -1)) {
          m_query.getLatencyMonitor().monitor(
              m_freeBuffer,
              m_windowResults[m_nextWindowToForward].m_latencyMark);
        }
      }

      /*
       * Before releasing the result buffer, increment bytes generated. It is
       * important all operators set the position of the buffer accordingly.
       * Assume that the start position is 0.
       */
      incTotalOutputBytes(length);

      /* Release the current slot */
      m_windowResults[m_nextWindowToForward].release(
          m_query.getId(), currentQuery->isMostDownstream(), m_freeBuffer,
          m_nextToForward, m_numberOfSlots, m_slotsToRelease);
      m_reservedSlots--;

      /* Increment next */
      m_nextWindowToForward += 1;
      if (m_nextWindowToForward == m_numberOfWindowSlots)
        m_nextWindowToForward = 0;
      m_forwardedWindows++;

      /* Check if next is ready to be pushed */
      oldVal = m_windowResults[m_nextWindowToForward].m_slot.load();
      if (oldVal < 3) {
        busy = false;
      }
    }

    if (debug) {
      std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id()
                << " end forwarding: " << m_nextToAggregate
                << " m_nextToAggregate " << m_nextToForward
                << " m_nextToForward " << m_forwardedWindows
                << " forwardedWindows " << m_nextWindowToForward
                << " nextWindowToForward " << m_nextToAggregateWindows
                << " nextToAggregateWindows " << std::endl;
    }

    /* Thread exit critical section */
    m_forwardLock.unlock();

  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

void ResultHandler::setAggregateOperator(AggregateOperatorCode *aggrOperator) {
  std::cout << "[DBG] set aggregate operator" << std::endl;
  m_aggrOperator = aggrOperator;
}

bool ResultHandler::containsFragmentedWindows() { return m_hasWindowFragments; }

ResultHandler::~ResultHandler() = default;