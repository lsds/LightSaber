#include "result/ResultHandler.h"

#if defined(TCP_OUTPUT)
#include <snappy.h>
#endif
#include <xmmintrin.h>

#include <algorithm>
#include <functional>
#include <stdexcept>

#include "buffers/PartialWindowResultsFactory.h"
#include "buffers/QueryBuffer.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "checkpoint/LineageGraphFactory.h"
#include "dispatcher/ITaskDispatcher.h"
#include "dispatcher/JoinTaskDispatcher.h"
#include "dispatcher/TaskDispatcher.h"
#include "monitors/LatencyMonitor.h"
#include "result/PartialResultSlot.h"
#include "tasks/WindowBatch.h"
#include "utils/Query.h"
#include "utils/QueryApplication.h"
#include "utils/QueryConfig.h"
#include "utils/QueryOperator.h"
#include "utils/Utils.h"

ResultHandler::ResultHandler(Query &query, QueryBuffer &freeBuffer1, QueryBuffer &freeBuffer2, bool hasWindowFragments, bool useParallelMerge) :
    m_query(query),
    m_freeBuffer1(freeBuffer1),
    m_freeBuffer2(freeBuffer2),
    m_hasWindowFragments(hasWindowFragments),
    m_useParallelMerge(useParallelMerge),
    m_maxTaskId(0),
    m_nextToForward(0),
    m_nextWindowToForward(0),
    m_nextToAggregate(0),
    m_totalOutputBytes(0L),
    m_numberOfSlots(!query.getConfig() ? SystemConf::getInstance().SLOTS : query.getConfig()->getNumberOfSlots()),
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
      if (j >= 0)
        m_results[j].connectTo(&m_results[i]);
    }
    m_results[m_numberOfSlots - 1].connectTo(&m_results[0]);
  } else {
    for (int i = 0, j = i - 1; i < m_numberOfSlots; i++, j++) {
      m_resultsWithoutFrags[i].m_index = i;
    }
  }
}

long ResultHandler::getTotalOutputBytes() {
  return m_totalOutputBytes;
}

void ResultHandler::incTotalOutputBytes(int bytes) {
  m_totalOutputBytes += ((long) bytes);
}

void ResultHandler::setupSocket() {
#if defined(TCP_OUTPUT)
  if (m_compressOutput) {
    //m_compressBuffer = UnboundedQueryBufferFactory::getInstance().newInstance(0);
    m_compressBuffers.resize(SystemConf::getInstance().WORKER_THREADS);
    for (auto &b: m_compressBuffers) {
      b = ByteBuffer(SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE);
    }
  }
  struct sockaddr_in serv_addr {};
  if ((m_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    throw std::runtime_error("error: Socket creation error");
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT);

  std::cout << "Setting up output socket: " << SystemConf::getInstance().REMOTE_CLIENT << std::endl;
  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, SystemConf::getInstance().REMOTE_CLIENT.c_str(),
                &serv_addr.sin_addr) <= 0) {
    throw std::runtime_error("error: Invalid address/ Address not supported");
  }

  if (connect(m_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    throw std::runtime_error("error: Connection Failed");
  }

#elif defined(RDMA_OUTPUT)
  // Create new context
  m_context = new infinity::core::Context();
  // Create a queue pair
  m_qpFactory = new infinity::queues::QueuePairFactory(m_context);
  std::cout << "Connecting to remote node " << SystemConf::getInstance().REMOTE_CLIENT << "..." << std::endl;
  m_qp = m_qpFactory->connectToRemoteHost(SystemConf::getInstance().REMOTE_CLIENT.c_str(), PORT);
  std::cout << "Connected to remote node " << SystemConf::getInstance().REMOTE_CLIENT << std::endl;
  m_sendBuffer = new infinity::memory::Buffer(m_context, SystemConf::getInstance().OUTPUT_BUFFER_SIZE * sizeof(char));
  m_receiveBuffer = new infinity::memory::Buffer(m_context, sizeof(char));
  m_context->postReceiveBuffer(m_receiveBuffer);

  std::cout <<"Sending first message" << std::endl;
  m_qp->send(m_sendBuffer, sizeof(char), m_context->defaultRequestToken);
  m_context->defaultRequestToken->waitUntilCompleted();
#endif
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
  auto freePtr1 = batch->getFreePointer();
  auto freePtr2 = batch->getSecondFreePointer();
  auto prevFreePtr1 = batch->getPrevFreePointer();
  auto prevFreePtr2 = batch->getPrevSecondFreePointer();
  auto latencyMark = batch->getLatencyMark();
  auto taskType = batch->getTaskType();

  if (taskId < 0) { /* Invalid task id */
    return;
  }

  int idx = ((taskId - 1) % m_numberOfSlots);
  try {
    auto oldVal = -1;
    if (taskType == TaskType::PROCESS || taskType == TaskType::ONLY_PROCESS) {
      while (
          !m_resultsWithoutFrags[idx].m_slot.compare_exchange_weak(oldVal, 0)) {
        std::cout << "warning: result collector (" << std::this_thread::get_id()
                  << ") blocked: query " +
                         std::to_string(currentQuery->getId()) + " task " +
                         std::to_string(taskId) + " slot " + std::to_string(idx)
                  << std::endl;
        _mm_pause();
      }

      m_resultsWithoutFrags[idx].m_freePointer1 = freePtr1;
      m_resultsWithoutFrags[idx].m_freePointer2 = freePtr2;
      m_resultsWithoutFrags[idx].m_prevFreePointer1 = prevFreePtr1;
      m_resultsWithoutFrags[idx].m_prevFreePointer2 = prevFreePtr2;
      m_resultsWithoutFrags[idx].m_result = result;
      m_resultsWithoutFrags[idx].m_latch = 0;
      m_resultsWithoutFrags[idx].m_latencyMark = latencyMark;
      if (SystemConf::getInstance().LINEAGE_ON) {
        m_resultsWithoutFrags[idx].m_graph = std::move(batch->getLineageGraph());
        m_resultsWithoutFrags[idx].m_freeOffset1 = batch->getStreamEndPointer();
        // todo: fix second offset here
        batch->getLineageGraph().reset();
      }
      m_resultsWithoutFrags[idx].m_taskId = taskId;

      /* No other thread can modify this slot. */
      m_resultsWithoutFrags[idx].m_slot.store(1);
      updateMaximumTaskId(taskId);
    }

    if (taskType == TaskType::PROCESS || taskType == TaskType::FORWARD || taskType == TaskType::MERGE_FORWARD) {
      /* Forward and free */
      if (!m_forwardLock.try_lock()) return;

      /* No other thread can enter this section */
      /* Is slot `next` occupied? */
      oldVal = 1;
      if (!m_resultsWithoutFrags[m_nextToForward]
               .m_slot.compare_exchange_strong(oldVal, 2)) {
        m_forwardLock.unlock();
        return;
      }

      bool busy = true;
      while (busy) {
        auto buffer = m_resultsWithoutFrags[m_nextToForward].m_result;
        int length = buffer->getPosition();

        /* Forward results */
        if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
          if (SystemConf::getInstance().LINEAGE_ON && length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
            std::cout << "warning: dropping duplicate results for query " + std::to_string(m_query.getId())
                + " with offset lower than " + std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;
          } else {
            /* Forward the latency mark downstream... */
            if (SystemConf::getInstance().LATENCY_ON &&
                (m_resultsWithoutFrags[m_nextToForward].m_latencyMark != -1)) {
              long t1 =
                  m_resultsWithoutFrags[m_nextToForward]
                      .m_latencyMark;  //(long) Utils::getSystemTimestamp (freeBuffer.getLong (resultsWithoutFrags[m_nextToForward].latencyMark));
              long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
              buffer->putLong(0, Utils::pack(t1, t2));
            }

            if (SystemConf::getInstance().LINEAGE_ON && (m_resultsWithoutFrags[m_nextToForward].m_graph || m_graph)) {
              if (m_graph && m_resultsWithoutFrags[m_nextToForward].m_graph) {
                m_resultsWithoutFrags[m_nextToForward].m_graph->mergeGraphs(m_graph);
                m_graph.reset();
              } else if (m_graph) {
                m_resultsWithoutFrags[m_nextToForward].m_graph = m_graph;
                m_graph.reset();
              }
              m_resultsWithoutFrags[m_nextToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
            }

            int nextQuery = m_resultsWithoutFrags[m_nextToForward].m_latch;
            for (int q = nextQuery;
                 q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
              if (currentQuery->getDownstreamQuery(q) != nullptr) {
                bool success = false;
                auto dispatcher =
                    currentQuery->getDownstreamQuery(q)->getTaskDispatcher();
                if (m_query.getIsLeft()) {
                  auto upstream = currentQuery->getDownstreamQuery(q)
                                      ->getNumberOfUpstreamQueries();
                  success =
                      (upstream == 1)
                          ? dispatcher->tryDispatchToFirstStream(
                                buffer->getBufferRaw(), length,
                                m_resultsWithoutFrags[m_nextToForward]
                                    .m_latencyMark,
                                m_resultsWithoutFrags[m_nextToForward].m_graph)
                          : dispatcher->tryDispatchSerialToFirstStream(
                                buffer->getBufferRaw(), length, m_forwardId,
                                m_resultsWithoutFrags[m_nextToForward]
                                    .m_latencyMark,
                                m_resultsWithoutFrags[m_nextToForward].m_graph);
                } else {
                  success = currentQuery->getDownstreamQuery(q)
                                ->getTaskDispatcher()
                                ->tryDispatchSerialToSecondStream(
                                    buffer->getBufferRaw(), length, m_forwardId,
                                    m_resultsWithoutFrags[m_nextToForward]
                                        .m_latencyMark, m_resultsWithoutFrags[m_nextToForward].m_graph);
                }

                if (!success) {
                  std::cout << "[DBG] failed to forward results from query "
                            << std::to_string(currentQuery->getId())
                            << " to next query "
                            << std::to_string(
                                   currentQuery->getDownstreamQuery(q)->getId())
                            << "..." << std::endl;
                  m_resultsWithoutFrags[m_nextToForward].m_latch = q;
                  m_resultsWithoutFrags[m_nextToForward].m_slot.store(1);
                  m_forwardLock.unlock();
                  return;
                } else {
                  m_resultsWithoutFrags[m_nextToForward].m_graph.reset();
                  m_forwardId++;
                }
              }
            }
          }
        } else if (length > 0 && currentQuery->isMostDownstream()) {
#if defined(TCP_OUTPUT)
          // send data over tcp to a remote sink
          if (SystemConf::getInstance().LINEAGE_ON) {
            m_graph->serialize();
            auto vecSize = m_graph->m_clockVector.size();
            send(m_sock, m_graph->m_clockVector.data(), vecSize, 0);
          }
          //if (!m_compressOutput) {
            send(m_sock, buffer->getBufferRaw(), length, 0);
          //} else {
          //  size_t output_length;
          //  snappy::RawCompress(buffer->getBufferRaw(), length,
          //                      m_compressBuffer->getBuffer().data(), &output_length);
          //  send(m_sock, m_compressBuffer->getBuffer().data(), output_length, 0);
          //}
          m_forwardId++;
#elif defined(RDMA_OUTPUT)
          infinity::requests::RequestToken requestToken(m_context);
          if (SystemConf::getInstance().LINEAGE_ON) {
            m_graph->serialize();
            auto vecSize = m_graph->m_clockVector.size();
            std::memcpy(m_sendBuffer->getData(), m_graph->m_clockVector.data(), vecSize);
            m_qp->send(m_sendBuffer, vecSize, &requestToken);
          }
          if (length < m_sendBuffer->getSizeInBytes()) {
            std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw(), length);
            m_qp->send(m_sendBuffer, length, &requestToken);
            requestToken.waitUntilCompleted();
          } else {
            std::cout << "[DBG] sending with RDMA " + std::to_string(length) + " > " + std::to_string(m_sendBuffer->getSizeInBytes())<< std::endl;
            auto curLength = std::min(length, (int)m_sendBuffer->getSizeInBytes());
            auto maxLength = length;
            while (maxLength < length) {
              std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw(), curLength);
              m_qp->send(m_sendBuffer, curLength, &requestToken);
              requestToken.waitUntilCompleted();
              maxLength += curLength;
              curLength = std::min(length-maxLength, (int)m_sendBuffer->getSizeInBytes());
            }
          }
          m_forwardId++;
#endif
          if (SystemConf::getInstance().LINEAGE_ON) {
            if (m_resultsWithoutFrags[m_nextToForward].m_graph) {
              m_resultsWithoutFrags[m_nextToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
            }
            if (length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
              std::cout << "warning: dropping duplicate results for query " +
                               std::to_string(m_query.getId()) + " with offset lower than " +
                               std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;
            }
          }
        }

        /* Forward to the distributed API */
        /* Measure latency */
        if (currentQuery->isMostDownstream()) {
          if (SystemConf::getInstance().LATENCY_ON &&
              (m_resultsWithoutFrags[m_nextToForward].m_latencyMark != -1
               && m_resultsWithoutFrags[m_nextToForward].m_latencyMark != 0)) { // when we have pipelined operators many 0 marks arrive
            m_query.getLatencyMonitor().monitor(
                m_freeBuffer1,
                m_resultsWithoutFrags[m_nextToForward].m_latencyMark);
          }
        }

        /*
       * Before releasing the result buffer, increment bytes generated. It is  important all operators set the position of the buffer accordingly. Assume that the start position is 0.
         */
        incTotalOutputBytes(length);
        buffer->clear(); // reset position
        PartialWindowResultsFactory::getInstance().free(buffer->getThreadId(),buffer);

        /* Free input buffer */
        auto fPointer1 = m_resultsWithoutFrags[m_nextToForward].m_freePointer1;
        if (fPointer1 != INT_MIN) m_freeBuffer1.free(fPointer1);
        auto fPointer2 = m_resultsWithoutFrags[m_nextToForward].m_freePointer2;
        if (fPointer2 != INT_MIN) m_freeBuffer2.free(fPointer2);

        if (SystemConf::getInstance().LINEAGE_ON && m_resultsWithoutFrags[m_nextToForward].m_graph) {
          if (currentQuery->isMostDownstream()) {
            if (SystemConf::getInstance().CHECKPOINT_ON) {
              if (!m_checkpointGraph) {
                m_checkpointGraph = LineageGraphFactory::getInstance().newInstance();
              }
              m_checkpointGraph->advanceOffsets(m_resultsWithoutFrags[m_nextToForward].m_graph);
              m_query.getParent()->getCheckpointCoordinator()->tryToPurgeCheckpoint(m_checkpointGraph);
            }
            m_resultsWithoutFrags[m_nextToForward].m_graph->freePersistentState(m_query.getId());
            m_resultsWithoutFrags[m_nextToForward].freeGraph();
          } else if (m_resultsWithoutFrags[m_nextToForward].m_graph.use_count() == 1) {
            if (m_graph) {
              m_graph->mergeGraphs(m_resultsWithoutFrags[m_nextToForward].m_graph);
            } else {
              m_graph = m_resultsWithoutFrags[m_nextToForward].m_graph;
            }
            m_resultsWithoutFrags[m_nextToForward].m_graph.reset();
          }
        }

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
    }

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

    auto oldVal = -1;
    // First try to merge partial results and then if we have complete result try to forward them
    if (taskType == TaskType::PROCESS || taskType == TaskType::ONLY_PROCESS) {
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
      if (SystemConf::getInstance().LINEAGE_ON) {
        m_results[idx].m_graph = std::move(batch->getLineageGraph());
        m_results[idx].m_freeOffset = batch->getStreamEndPointer();
        batch->getLineageGraph().reset();
      }
      m_results[idx].init(batch);
      /* No other thread can modify this slot. */
      m_results[idx].m_slot.store(1);
      updateMaximumTaskId(taskId);
    }

    int mergeCounter = 0;
    if (taskType == TaskType::PROCESS || taskType == TaskType::MERGE || taskType == TaskType::MERGE_FORWARD) {
      /* Try aggregate slots pair-wise */
      if (m_mergeLock.try_lock()) {
        PartialResultSlot *currentSlot;
        PartialResultSlot *nextSlot;
        while (true) {
          currentSlot = &m_results[m_nextToAggregate];
          nextSlot = currentSlot->m_next;

          int currentSlotFlag = currentSlot->m_slot.load();
          if (currentSlotFlag > 1 && currentSlotFlag != 5)
            throw std::runtime_error(
                "error: inconsistent state in next result slot to aggregate with currentSlotFlag " +
                std::to_string(currentSlotFlag));
          if (currentSlotFlag < 1 || currentSlotFlag == 5) break;
          int nextSlotFlag = nextSlot->m_slot.load();
          if (nextSlotFlag > 1 && nextSlotFlag != 5) {
            if (nextSlotFlag == 4) break;
            debugAggregateAndForwardAndFree();
            throw std::runtime_error(
                "error: inconsistent state in next result slot to aggregate with nextSlotFlag " +
                std::to_string(nextSlotFlag));
          }
          if (nextSlotFlag < 1 || nextSlotFlag == 5) break;

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
          //if (SystemConf::getInstance().CHECKPOINT_ON && m_stopMerging && mergeCounter++ == 3) {
          //  break;
          //}
        }
        m_mergeLock.unlock();
      }
    }

    if (taskType == TaskType::PROCESS || taskType == TaskType::FORWARD || taskType == TaskType::MERGE_FORWARD) {
      //if (SystemConf::getInstance().CHECKPOINT_ON && m_stopMerging)
      //  return;
      /* Forward and free */
      if (!m_forwardLock.try_lock()) return;

      /* No other thread can enter this section */
      /* Is slot `next` occupied? */
      oldVal = 3;
      if (!m_results[m_nextToForward].m_slot.compare_exchange_strong(oldVal,
                                                                     4)) {
        m_forwardLock.unlock();
        return;
      }

      bool busy = true;
      while (busy) {
        auto buffer = m_results[m_nextToForward].m_completeWindows;
        int length = buffer->getPosition();

        //auto buff = ((long*)buffer->getBufferRaw());
        //for (int ii = 0; ii < length/16; ++ii) {
        //  std::cout << std::to_string(buff[ii*2]) << std::endl;
        //}

        /* Forward results */
        if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
          if (SystemConf::getInstance().LINEAGE_ON && length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
            std::cout << "warning: dropping duplicate results for query " + std::to_string(m_query.getId())
                + " with offset lower than " + std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;

          } else {
            /* Forward the latency mark downstream... */
            if (SystemConf::getInstance().LATENCY_ON &&
                (m_results[m_nextToForward].m_latencyMark != -1)) {
              long t1 =
                  m_results[m_nextToForward]
                      .m_latencyMark;  //(long) Utils::getSystemTimestamp (freeBuffer.getLong (results[m_nextToForward].latencyMark));
              long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
              buffer->putLong(0, Utils::pack(t1, t2));
            }

            if (SystemConf::getInstance().LINEAGE_ON && (m_results[m_nextToForward].m_graph || m_graph)) {
              if (m_graph && m_results[m_nextToForward].m_graph) {
                m_results[m_nextToForward].m_graph->mergeGraphs(m_graph);
                m_graph.reset();
              } else if (m_graph) {
                m_results[m_nextToForward].m_graph = m_graph;
                m_graph.reset();
              }
              m_results[m_nextToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
            }

            int nextQuery = m_results[m_nextToForward].m_latch;
            for (int q = nextQuery;
                 q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
              if (currentQuery->getDownstreamQuery(q) != nullptr) {
                bool success = false;
                auto dispatcher =
                    currentQuery->getDownstreamQuery(q)->getTaskDispatcher();
                if (m_query.getIsLeft()) {
                  auto upstream = currentQuery->getDownstreamQuery(q)
                                      ->getNumberOfUpstreamQueries();
                  success =
                      (upstream == 1)
                          ? dispatcher->tryDispatchToFirstStream(
                                buffer->getBufferRaw(), length,
                                m_results[m_nextToForward].m_latencyMark,
                                m_results[m_nextToForward].m_graph)
                          : dispatcher->tryDispatchSerialToFirstStream(
                                buffer->getBufferRaw(), length, m_forwardId,
                                m_results[m_nextToForward].m_latencyMark,
                                m_results[m_nextToForward].m_graph);
                } else {
                  success = dispatcher->tryDispatchSerialToSecondStream(
                      buffer->getBufferRaw(), length, m_forwardId,
                      m_results[m_nextToForward].m_latencyMark,
                      m_results[m_nextToForward].m_graph);
                }

                if (!success) {
                  std::cout << "[DBG] ForwardAndFree: failed to forward results from query "
                            << std::to_string(currentQuery->getId())
                            << " to next query "
                            << std::to_string(
                                   currentQuery->getDownstreamQuery(q)->getId())
                            << "..." << std::endl;
                  m_results[m_nextToForward].m_latch = q;
                  m_results[m_nextToForward].m_slot.store(3);
                  m_forwardLock.unlock();
                  return;
                } else {
                  m_results[m_nextToForward].m_graph.reset();
                  m_forwardId++;
                }
              }
            }
          }
        } else if (length > 0 && currentQuery->isMostDownstream()) {
#if defined(TCP_OUTPUT)
          // send data over tcp to a remote sink
          if (SystemConf::getInstance().LINEAGE_ON) {
            m_graph->serialize();
            auto vecSize = m_graph->m_clockVector.size();
            send(m_sock, m_graph->m_clockVector.data(), vecSize, 0);
          }
          //if (!m_compressOutput) {
            send(m_sock, buffer->getBufferRaw(), length, 0);
          //} else {
          //  size_t output_length;
          //  snappy::RawCompress(buffer->getBufferRaw(), length,
          //                      m_compressBuffer->getBuffer().data(), &output_length);
          //  send(m_sock, m_compressBuffer->getBuffer().data(), output_length, 0);
          //}
          m_forwardId++;
#elif defined(RDMA_OUTPUT)
          infinity::requests::RequestToken requestToken(m_context);
          //std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw(), length);
          if (SystemConf::getInstance().LINEAGE_ON) {
            m_graph->serialize();
            auto vecSize = m_graph->m_clockVector.size();
            std::memcpy(m_sendBuffer->getData(), m_graph->m_clockVector.data(), vecSize);
            m_qp->send(m_sendBuffer, vecSize, &requestToken);
          }
          if (length < m_sendBuffer->getSizeInBytes()) {
            std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw(), length);
            m_qp->send(m_sendBuffer, length, &requestToken);
            requestToken.waitUntilCompleted();
          } else {
            std::cout << "[DBG] sending with RDMA " + std::to_string(length) + " > " + std::to_string(m_sendBuffer->getSizeInBytes())<< std::endl;
            auto curLength = std::min(length, (int)m_sendBuffer->getSizeInBytes());
            auto maxLength = 0;
            while (maxLength < length) {
              std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw(), curLength);
              m_qp->send(m_sendBuffer, curLength, &requestToken);
              requestToken.waitUntilCompleted();
              maxLength += curLength;
              curLength = std::min(length-maxLength, (int)m_sendBuffer->getSizeInBytes());
            }
          }
          m_forwardId++;
#endif
          if (SystemConf::getInstance().LINEAGE_ON) {
            if (m_results[m_nextToForward].m_graph) {
              m_results[m_nextToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
            }
            // drop duplicates
            if (length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
              std::cout << "warning: dropping duplicate results for query " + std::to_string(m_query.getId())
              + " with offset lower than " + std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;
            }
          }
        }

        /* Forward to the distributed API */
        /* Measure latency */
        if (currentQuery->isMostDownstream()) {
          if (SystemConf::getInstance().LATENCY_ON &&
              (m_results[m_nextToForward].m_latencyMark != -1
               && m_results[m_nextToForward].m_latencyMark != 0)) { // when we have pipelined operators many 0 marks arrive
            //std::cout << "The latency mark arrived for monitoring is " + std::to_string(m_results[m_nextToForward].m_latencyMark) << std::endl;
            m_query.getLatencyMonitor().monitor(
                m_freeBuffer1, m_results[m_nextToForward].m_latencyMark);
          }
        }

        /*
       * Before releasing the result buffer, increment bytes generated. It is  important all operators set the position of the buffer accordingly. Assume that the start position is 0.
         */
        incTotalOutputBytes(length);

        /* Free input buffer */
        auto fPointer = m_results[m_nextToForward].m_freePointer;
        if (fPointer != INT_MIN) m_freeBuffer1.free(fPointer);

        if (SystemConf::getInstance().LINEAGE_ON && m_results[m_nextToForward].m_graph) {
          if (currentQuery->isMostDownstream()) {
            if (SystemConf::getInstance().CHECKPOINT_ON) {
              if (!m_checkpointGraph) {
                m_checkpointGraph = LineageGraphFactory::getInstance().newInstance();
              }
              m_checkpointGraph->advanceOffsets(m_results[m_nextToForward].m_graph);
              m_query.getParent()->getCheckpointCoordinator()->tryToPurgeCheckpoint(m_checkpointGraph);
            }
            m_results[m_nextToForward].m_graph->freePersistentState(m_query.getId());
            m_results[m_nextToForward].freeGraph();
          } else if (m_results[m_nextToForward].m_graph.use_count() == 1) {
            if (m_graph) {
              m_graph->mergeGraphs(m_results[m_nextToForward].m_graph);
            } else {
              m_graph = m_results[m_nextToForward].m_graph;
            }
            m_results[m_nextToForward].m_graph.reset();
          }
        }

        m_results[m_nextToForward].release();
        /* Release the current slot */
        m_results[m_nextToForward].m_slot.store(-1);

        /* Increment next */
        m_nextToForward += 1;
        if (m_nextToForward == m_numberOfSlots) m_nextToForward = 0;

        if (SystemConf::getInstance().CHECKPOINT_ON && m_stopMerging && mergeCounter++ == 5) {
          break;
        }

        /* Check if next is ready to be pushed */
        oldVal = 3;
        if (!m_results[m_nextToForward].m_slot.compare_exchange_strong(oldVal,
                                                                       4)) {
          busy = false;
        }
      }
      /* Thread exit critical section */
      m_forwardLock.unlock();
    }
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

void ResultHandler::debugAggregateAndForwardAndFree() {
  std::string str;
  size_t idx = 0;
  for (auto &slot : m_results) {
    str.append(std::to_string(idx) + ": slot " +
        std::to_string(slot.m_slot.load()) + " taskId " +
        std::to_string(slot.m_taskId) + " windowFrags " +
        std::to_string(slot.getNumberOfWindowFragments(true)) + "\n");
    idx++;
  }
  std::cout << "[DBG] warning: \n" << str << std::endl;
}

/*
 * Flags:
 *  -1: slot is free
 *   0: slot is being populated by a thread
 *   1: slot is occupied, but "unlocked", thus it can be aggregated with its next one"
 *   2: slot is occupied, but "locked", thus it is being processed
 * Extra Aggregation Flags:
 *   3: slot is occupied with BufferPtrs result, but "unlocked", thus it is ready to be forwarded
 *   4: slot is occupied with Buffer result, but "unlocked", thus it is ready to be forwarded
 *   5: slot is occupied, but "locked", thus the result are being forwarded
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

    auto oldVal = -1;
    // First try to merge partial results and then if we have complete result try to forward them
    if (taskType == TaskType::PROCESS || taskType == TaskType::ONLY_PROCESS) {
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
      if (SystemConf::getInstance().LINEAGE_ON) {
        m_results[taskIdx].m_graph = std::move(batch->getLineageGraph());
        m_results[taskIdx].m_freeOffset = batch->getStreamEndPointer();
        batch->getLineageGraph().reset();
      }
      m_results[taskIdx].init(batch);
      /* No other thread can modify this slot. */
      m_results[taskIdx].m_slot.store(1);
      //std::cout << "[DBG] setting slot " + std::to_string(m_results[taskIdx].m_index) + " to 2" << std::endl;
      updateMaximumTaskId(taskId);
    }

    if (taskType == TaskType::ONLY_PROCESS)
      return;

    int mergeCounter = 0;

    /* Try assign window ids to partials in slots pair-wise */
    if (m_prepareMergeLock.try_lock()) {
      PartialResultSlot *currentSlot;
      PartialResultSlot *nextSlot;
      while (true) {
        currentSlot = &m_results[m_nextToAggregate];
        nextSlot = currentSlot->m_next;

        int currentSlotFlag = currentSlot->m_slot.load();
        int windowsToStore = 0;
        windowsToStore += (currentSlot->m_openingWindows) ? currentSlot->m_openingWindows->numberOfWindows() : 0;
        windowsToStore += (currentSlot->m_completeWindows) ? currentSlot->m_completeWindows->numberOfWindows() : 0;
        //if (currentSlotFlag > 1)
        //  throw std::runtime_error("error: invalid slot state equal to " + std::to_string(currentSlotFlag));
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
          std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " starts the window assignment with: "
                    <<
                    m_numberOfWindowSlots << " numberOfWindowSlots " <<
                    m_nextToAggregate << " nextToAggregate " <<
                    m_openingWindowsList.size() << " remaining opening windows " <<
                    m_currentWindowSlot << " currentWindowSlot " <<
                    currentSlot->m_closingWindows->numberOfWindows() << " closingWindows " <<
                    currentSlot->m_pendingWindows->numberOfWindows() << " pendingWindows " <<
                    currentSlot->m_completeWindows->numberOfWindows() << " completeWindows " <<
                    currentSlot->m_openingWindows->numberOfWindows() << " openingWindows " << std::endl;
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
          // when we reach a closing window, the window slot is ready to output a full result
          auto &closingWindows = currentSlot->m_closingWindows;
          if (closingWindows->numberOfWindows() > 0) {
            for (window = 0; window < closingWindows->numberOfWindows(); ++window) {
              if (m_openingWindowsList.empty()) {
                std::cout << "[DBG] ForwardAndFree: warning found additional "
                          << "closing window with current window slot "
                          << m_currentWindowSlot << std::endl;
                break;
              }
              auto firstOpeningWindow = m_openingWindowsList.front();
              latestWindowSlot = firstOpeningWindow;
              //while (!(m_windowResults[firstOpeningWindow].slot.load() == 0 ||
              //        m_windowResults[firstOpeningWindow].slot.load() == 1))
              while (m_windowResults[firstOpeningWindow].m_slot.load() != 0)
                std::cout << "[DBG] ForwardAndFree [Warning]: closingWindow waiting for slot " << m_currentWindowSlot
                          << std::endl; // partial results are already in
              m_openingWindowsList.pop_front();

              m_windowResults[firstOpeningWindow].m_partialWindows.push_back(
                  //push(
                  //std::make_shared<PartialWindowResultsWrapper>(closingWindows, window, true));
                  closingWindows, window, true);
              //m_windowResults[firstOpeningWindow].slot.store(2);
              //m_windowResults[firstOpeningWindow].setHasAllBatches();

              // let workers know that there is work
              m_availableSlots.push(firstOpeningWindow);
            }
            //closingWindows.reset();
          }

          // insert complete windows if any, which are ready for forwarding
          auto &completeWindows = currentSlot->m_completeWindows;
          if (completeWindows->numberOfWindows() > 0) {
            latestWindowSlot = m_currentWindowSlot;
            while (m_windowResults[m_currentWindowSlot].m_slot.load() != -1)
              std::cout << "[DBG] ForwardAndFree [Warning]: completeWindow waiting for slot " << m_currentWindowSlot
                        << std::endl; // the slot must be empty
            m_windowResults[m_currentWindowSlot].m_completeWindows = completeWindows;
            // set that that the slot is ready to forward with Buffer result
            m_windowResults[m_currentWindowSlot].m_slot.store(3);
            m_windowResults[m_currentWindowSlot].m_completeWindowsStartPos = 0;
            m_windowResults[m_currentWindowSlot].m_length = completeWindows->getPosition();
            //m_windowResults[m_currentWindowSlot].setHasComplete();
            m_currentWindowSlot++;
            if (m_currentWindowSlot == m_numberOfWindowSlots)
              m_currentWindowSlot = 0;
            newWindows++;
          }

          // insert pending windows if any
          auto &pendingWindows = currentSlot->m_pendingWindows;
          if (pendingWindows->numberOfWindows() > 0) {
            //auto pendingWindow = std::make_shared<PartialWindowResultsWrapper>(pendingWindows, 0, false);
            //for (window = 0; window < pendingWindows->numberOfWindows(); ++window) {
            for (auto &firstOpeningWindow : m_openingWindowsList) {
              latestWindowSlot = firstOpeningWindow;
              while (m_windowResults[firstOpeningWindow].m_slot.load() != 0)
                std::cout << "[DBG] ForwardAndFree [Warning]: pendingWindow waiting for slot " << m_currentWindowSlot
                          << std::endl; // partial results are already in
              m_windowResults[firstOpeningWindow].m_partialWindows.push_back(
                  //push(pendingWindow);
                  pendingWindows, 0, false);
              //m_windowResults[firstOpeningWindow].slot.store(1);
            }
            //pendingWindows.reset();
          }

          // finally insert opening windows if they exist
          // and keep the earliest window
          auto &openingWindows = currentSlot->m_openingWindows;
          if (openingWindows->numberOfWindows() > 0) {
            for (window = 0; window < openingWindows->numberOfWindows(); ++window) {
              latestWindowSlot = m_currentWindowSlot;
              while (m_windowResults[m_currentWindowSlot].m_slot.load() != -1)
                std::cout << "[DBG] ForwardAndFree [Warning]: openingWindow waiting for slot " << m_currentWindowSlot
                          << std::endl; // the slot must be empty
              m_openingWindowsList.push_back(m_currentWindowSlot);

              if (m_windowResults[m_currentWindowSlot].m_partialWindows.size() == 0)
                m_windowResults[m_currentWindowSlot].m_partialWindows.set_capacity(4);

              m_windowResults[m_currentWindowSlot].m_partialWindows.push_back(
                  //push(
                  //std::make_shared<PartialWindowResultsWrapper>(openingWindows, window, false));
                  openingWindows, window, false);

              // reuse the completeWindowsBuffer here
              m_windowResults[m_currentWindowSlot].m_completeWindows = currentSlot->m_completeWindows;
              m_windowResults[m_currentWindowSlot].m_completeWindowsStartPos =
                  currentSlot->m_completeWindows->getPosition();
              m_windowResults[m_currentWindowSlot].m_completeWindow =
                  window; //currentSlot->completeWindows->numberOfWindows() + window;

              // set latency mark
              m_windowResults[m_currentWindowSlot].m_latencyMark = currentSlot->m_latencyMark;

              m_windowResults[m_currentWindowSlot].m_slot.store(0);
              m_currentWindowSlot++;
              if (m_currentWindowSlot == m_numberOfWindowSlots)
                m_currentWindowSlot = 0;
              newWindows++;
            }
            //openingWindows.reset();
          }

          m_insertedWindows += newWindows;
          m_reservedSlots.fetch_add(newWindows);

          // sanity check
          /*if (currentSlot->completeWindows->numberOfWindows() > 0 &&
          currentSlot->openingWindows->numberOfWindows() + 2 != currentSlot->completeWindows.use_count()) {
              std::string errorMsg = "error: wrong ref counters after assigning window ids for completeWindows " +
                      std::to_string(currentSlot->openingWindows->numberOfWindows()) + " " +
                      std::to_string(currentSlot->completeWindows.use_count());
              throw std::runtime_error(errorMsg);
          }
          if (currentSlot->openingWindows->numberOfWindows() > 0 &&
              currentSlot->openingWindows->numberOfWindows() + 1 != currentSlot->openingWindows.use_count()) {
              std::string errorMsg = "error: wrong ref counters after assigning window ids for openingWindows " +
                                     std::to_string(currentSlot->openingWindows->numberOfWindows()) + " " +
                                     std::to_string(currentSlot->openingWindows.use_count());
              throw std::runtime_error(errorMsg);
          }
          if (currentSlot->closingWindows->numberOfWindows() > 0 &&
              currentSlot->closingWindows->numberOfWindows() + 1 != currentSlot->closingWindows.use_count()) {
              std::string errorMsg = "error: wrong ref counters after assigning window ids for closingWindows " +
                                     std::to_string(currentSlot->closingWindows->numberOfWindows()) + " " +
                                     std::to_string(currentSlot->closingWindows.use_count());
              throw std::runtime_error(errorMsg);
          }
          if (currentSlot->pendingWindows->numberOfWindows() > 0 &&
              currentSlot->pendingWindows->numberOfWindows() + 1 != currentSlot->pendingWindows.use_count()) {
              std::string errorMsg = "error: wrong ref counters after assigning window ids for pendingWindows " +
                                     std::to_string(currentSlot->pendingWindows->numberOfWindows()) + " " +
                                     std::to_string(currentSlot->pendingWindows.use_count());
              throw std::runtime_error(errorMsg);
          }*/

          // set the end pointer on the result slot in order to free them
          if (latestWindowSlot == -1)
            throw std::runtime_error("error: invalid latestWindowSlot");
          m_windowResults[latestWindowSlot].setResultSlot(currentSlot);
          if (SystemConf::getInstance().LINEAGE_ON && !currentQuery->isMostDownstream()) {
            m_windowResults[latestWindowSlot].m_graph = std::move(currentSlot->m_graph);
          }

          // Free complete windows here!!!
          currentSlot->m_completeWindows.reset();

          /* Free input buffer */
          //int fPointer = currentSlot->freePointer;
          //if (fPointer != INT_MIN)
          //    freeBuffer.free(fPointer);
        }

        m_nextToAggregate = nextSlot->m_index;
        if (debug) {
          std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " ends the window assignment with: " <<
                    m_insertedWindows << " insertedWindows " <<
                    m_nextToAggregate << " nextToAggregate " <<
                    m_openingWindowsList.size() << " remaining opening windows " <<
                    m_currentWindowSlot << " currentWindowSlot " << std::endl;
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
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " start aggregating: " <<
                  nextWindowSlotForAggregation << " nextToAggregateWindows " <<
                  idx << " m_currentWindowSlot " << std::endl;
      }
#if defined(PREFETCH)
      // prefetch data here
      m_windowResults[idx].prefetch();
#endif
#if defined(TCP_OUTPUT)
      // compress output if needed
      if (m_query.isMostDownstream() && m_compressOutput) {
        m_windowResults[idx].aggregateAll(m_aggrOperator, pid, false);
        int length = m_windowResults[idx].m_length;
        if (length > 0) {
          size_t output_length;
          snappy::RawCompress(m_windowResults[idx].m_completeWindows->getBufferRaw() + m_windowResults[idx].m_completeWindowsStartPos, length,m_compressBuffers[pid].data(), &output_length);
          std::memcpy(m_windowResults[idx].m_completeWindows->getBufferRaw()  + m_windowResults[idx].m_completeWindowsStartPos,m_compressBuffers[pid].data(), output_length);
          m_windowResults[idx].m_length = output_length;
        }
        m_windowResults[idx].m_slot.store(3);
      } else {
        m_windowResults[idx].aggregateAll(m_aggrOperator, pid);
      }
#else
      m_windowResults[idx].aggregateAll(m_aggrOperator, pid);
#endif
      // set that that the slot is ready to forward with BufferPtrs
      //if (m_windowResults[idx].isReady()) {
      //    m_windowResults[idx].slot.store(3);
      //}

      if (debug) {
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " finished aggregating: " <<
                  nextWindowSlotForAggregation << " nextToAggregateWindows " <<
                  idx << " m_currentWindowSlot " << std::endl;
      }
      hasWork = m_availableSlots.try_pop(nextWindowSlotForAggregation);
    }

    // try to forward from m_nextWindowToForward: there are two types to forward
    //if (SystemConf::getInstance().CHECKPOINT_ON && m_stopMerging)
    //  return;

    /* Forward and free */
    if (!m_forwardLock.try_lock())
      return;

    /* Release any previous kept slots */
    auto iter = m_slotsToRelease.begin();
    while (iter != m_slotsToRelease.end()) {
      //std::cout << "[DBG] warning: releasing slots after checkpoint" << std::endl;
      auto slot = *iter;
      if(slot->m_slot.load() == 3) {
        //std::cout << "[DBG] warning: slot " + std::to_string(slot->m_index) + " released " << std::endl;
        if (SystemConf::getInstance().LINEAGE_ON && currentQuery->isMostDownstream()) {
          // todo: fix lineage merge for the parallel window merge approach
          if (m_results[m_nextToForward].m_graph) {
            if (SystemConf::getInstance().CHECKPOINT_ON) {
              if (!m_checkpointGraph) {
                m_checkpointGraph = LineageGraphFactory::getInstance().newInstance();
              }
              m_checkpointGraph->advanceOffsets(m_results[m_nextToForward].m_graph);
              m_query.getParent()->getCheckpointCoordinator()->tryToPurgeCheckpoint(m_checkpointGraph);
            }
            m_results[m_nextToForward].m_graph->freePersistentState(m_query.getId());
            m_results[m_nextToForward].freeGraph();
          }
          if (slot->m_graph) {
            slot->m_graph->freePersistentState(m_query.getId());
            slot->freeGraph();
          }
        }
        slot->release();
        // Free input buffer
        auto fPointer = slot->m_freePointer;
        if (fPointer != INT_MIN) m_freeBuffer1.free(fPointer);

        //std::cout << "[DBG] setting slot " + std::to_string(slot->m_index) + " to -1" << std::endl;
        slot->m_slot.store(-1);
        iter = m_slotsToRelease.erase(iter);
        m_nextToForward = (m_nextToForward+1) % m_numberOfSlots;
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
        std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " start forwarding: " <<
            m_nextWindowToForward << " nextWindowToForward " << std::endl;
      }

      auto &buffer = m_windowResults[m_nextWindowToForward].m_completeWindows;
      int startPos = m_windowResults[m_nextWindowToForward].m_completeWindowsStartPos;
      int length = m_windowResults[m_nextWindowToForward].m_length;
      int type = buffer->getType();

      //if (length)
      //  std::cout << length << std::endl;
      //auto buff = ((long *)(buffer->getBufferRaw() + startPos));
      //for (int ii = 0; ii < length / 32; ++ii) {
      //  std::cout << std::to_string(buff[ii * 4]) << std::endl;
      //}

      // Forward m_windowResults
      if (length > 0 && currentQuery->getNumberOfDownstreamQueries() > 0) {
        if (SystemConf::getInstance().LINEAGE_ON && length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
          std::cout << "warning: dropping duplicate results for query " + std::to_string(m_query.getId())
              + " with offset lower than " + std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;
        } else {
          // Forward the latency mark downstream...
          if (SystemConf::getInstance().LATENCY_ON &&
              (m_windowResults[m_nextWindowToForward].m_latencyMark != -1)) {
            if (type == 1)
              throw std::runtime_error(
                  "Latency is not supported for type 1 buffer yet.");
            long t1 =
                m_windowResults[m_nextWindowToForward]
                    .m_latencyMark;  //(long) Utils::getSystemTimestamp (freeBuffer.getLong (m_windowResults[m_nextWindowToForward].latencyMark));
            long t2 = (long)Utils::getTupleTimestamp(buffer->getLong(0));
            // std::cout << "Forwarding latency mark " + std::to_string(t1) + " merged as " + std::to_string(Utils::pack(t1, t2)) + " timestamp." << std::endl;
            buffer->putLong(0, Utils::pack(t1, t2));
          }

          if (SystemConf::getInstance().LINEAGE_ON && m_windowResults[m_nextWindowToForward].m_graph) {
            m_windowResults[m_nextWindowToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
          }

          int nextQuery = m_windowResults[m_nextWindowToForward].m_latch;
          for (int q = nextQuery;
               q < currentQuery->getNumberOfDownstreamQueries(); ++q) {
            if (currentQuery->getDownstreamQuery(q) != nullptr) {
              bool success = false;
              if (type == 0) {
                auto dispatcher =
                    currentQuery->getDownstreamQuery(q)->getTaskDispatcher();
                if (m_query.getIsLeft()) {
                  auto upstream = currentQuery->getDownstreamQuery(q)
                                      ->getNumberOfUpstreamQueries();
                  success =
                      (upstream == 1)
                          ? dispatcher->tryDispatchToFirstStream(
                                buffer->getBufferRaw() + startPos, length,
                                m_windowResults[m_nextWindowToForward]
                                    .m_latencyMark,
                                m_windowResults[m_nextWindowToForward].m_graph)
                          : dispatcher->tryDispatchSerialToFirstStream(
                                buffer->getBufferRaw() + startPos, length,
                                m_forwardId,
                                m_windowResults[m_nextWindowToForward]
                                    .m_latencyMark,
                                m_windowResults[m_nextWindowToForward].m_graph);
                } else {
                  //if (m_windowResults[m_nextWindowToForward].m_graph) {
                  //  std::cout << "a graph reached this point" << std::endl;
                  //}
                  success = dispatcher->tryDispatchSerialToSecondStream(
                      buffer->getBufferRaw() + startPos, length, m_forwardId,
                      m_windowResults[m_nextWindowToForward].m_latencyMark, m_windowResults[m_nextWindowToForward].m_graph);
                }
              } else {
                // success = currentQuery->getDownstreamQuery(q)->getTaskDispatcher()->tryDispatch(buffer->getBufferPtrs()[0], length);
                throw std::runtime_error(
                    "Forwarding for type 1 buffer is not supported yet.");
              }
              if (!success) {
                std::cout << "[DBG] WindowsForwardAndFree: failed to forward results from query "
                          << std::to_string(currentQuery->getId())
                          << " to next query "
                          << std::to_string(
                                 currentQuery->getDownstreamQuery(q)->getId())
                          << "..." << std::endl;
                m_windowResults[m_nextWindowToForward].m_latch = q;
                m_windowResults[m_nextWindowToForward].m_slot.store(oldVal);
                m_forwardLock.unlock();
                return;
              } else {
                m_windowResults[m_nextWindowToForward].m_graph.reset();
                if (!m_windowResults[m_nextWindowToForward].m_resSlots.empty())
                  m_forwardId++;
              }
            }
          }
        }
      } else if (length > 0 &&  currentQuery->isMostDownstream()) {
#if defined(TCP_OUTPUT)
        // send data over tcp to a remote sink
        if (SystemConf::getInstance().LINEAGE_ON) {
            m_graph->serialize();
            auto vecSize = m_graph->m_clockVector.size();
            send(m_sock, m_graph->m_clockVector.data(), vecSize, 0);
        }
        //if (!m_compressOutput) {
          send(m_sock, buffer->getBufferRaw() + startPos, length, 0);
        //} else {
        //  size_t output_length;
        //  snappy::RawCompress(buffer->getBufferRaw() + startPos, length,
        //                      m_compressBuffer->getBuffer().data(), &output_length);
        //  send(m_sock, m_compressBuffer->getBuffer().data(), output_length, 0);
        //}
          m_forwardId++;

#elif defined(RDMA_OUTPUT)
        infinity::requests::RequestToken requestToken(m_context);
        //std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw() + startPos, length);
        //m_qp->send(m_sendBuffer, m_sendBuffer->getSizeInBytes(), &requestToken);
        //requestToken.waitUntilCompleted();
        if (SystemConf::getInstance().LINEAGE_ON) {
          m_graph->serialize();
          auto vecSize = m_graph->m_clockVector.size();
          std::memcpy(m_sendBuffer->getData(), m_graph->m_clockVector.data(), vecSize);
          m_qp->send(m_sendBuffer, vecSize, &requestToken);
        }
        if (length < m_sendBuffer->getSizeInBytes()) {
          std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw() + startPos, length);
          m_qp->send(m_sendBuffer, length, &requestToken);
          requestToken.waitUntilCompleted();
        } else {
          std::cout << "[DBG] sending with RDMA " + std::to_string(length) + " > " + std::to_string(m_sendBuffer->getSizeInBytes())<< std::endl;
          auto curLength = std::min(length, (int)m_sendBuffer->getSizeInBytes());
          auto maxLength = 0;
          auto cnter = 0;
          while (maxLength < length) {
            std::memcpy(m_sendBuffer->getData(), buffer->getBufferRaw() + startPos + maxLength, curLength);
            m_qp->send(m_sendBuffer, curLength, &requestToken);
            requestToken.waitUntilCompleted();
            maxLength += curLength;
            curLength = std::min(length-maxLength, (int)m_sendBuffer->getSizeInBytes());
            cnter++;
          }
          //std::cout << "[DBG] entered the loop " + std::to_string(cnter) << std::endl;
        }
        m_forwardId++;
#endif
        if (SystemConf::getInstance().LINEAGE_ON) {
          if (m_windowResults[m_nextWindowToForward].m_graph) {
            m_windowResults[m_nextWindowToForward].m_graph->setOutputPtr(m_query.getId(), m_totalOutputBytes + length);
          }
          if (length + m_totalOutputBytes <= m_query.getOperator()->getOutputPtr()) {
            std::cout << "warning: dropping duplicate results for query " + std::to_string(m_query.getId())
                + " with offset lower than " + std::to_string(m_query.getOperator()->getOutputPtr()) << std::endl;
          }
        }
      }

      /* Forward to the distributed API */
      /* Measure latency */
      if (currentQuery->isMostDownstream()) {
        if (SystemConf::getInstance().LATENCY_ON && (m_windowResults[m_nextWindowToForward].m_latencyMark != -1
            && m_windowResults[m_nextWindowToForward].m_latencyMark != 0)) { // when we have pipelined operators many 0 marks arrive
          //std::cout << "The latency mark arrived for monitoring is " + std::to_string(m_windowResults[m_nextWindowToForward].m_latencyMark) << std::endl;
          m_query.getLatencyMonitor().monitor(m_freeBuffer1, m_windowResults[m_nextWindowToForward].m_latencyMark);
        }
      }

      /*
       * Before releasing the result buffer, increment bytes generated. It is  important
       * all operators set the position of the buffer accordingly. Assume that the start
       * position is 0.
       */
      incTotalOutputBytes(length);

      /* Release the current slot */
      m_windowResults[m_nextWindowToForward].release(m_query.getId(), currentQuery->isMostDownstream(), m_freeBuffer1, m_nextToForward, m_numberOfSlots, m_slotsToRelease);
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
      if (SystemConf::getInstance().CHECKPOINT_ON && m_stopMerging && mergeCounter++ == 5) {
        break;
      }
    }

    if (debug) {
      std::cout << "[DBG] ForwardAndFree " << std::this_thread::get_id() << " end forwarding: " <<
                m_nextToAggregate << " m_nextToAggregate " <<
                m_nextToForward << " m_nextToForward " <<
                m_forwardedWindows << " forwardedWindows " <<
                m_nextWindowToForward << " nextWindowToForward " <<
                m_nextToAggregateWindows << " nextToAggregateWindows " << std::endl;
    }

    /* Thread exit critical section */
    m_forwardLock.unlock();

  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

void ResultHandler::updateMaximumTaskId(int value) {
  auto prev_value = m_maxTaskId.load();
  while (prev_value < value &&
         !m_maxTaskId.compare_exchange_weak(prev_value, value)) {
  }
}

void ResultHandler::setAggregateOperator(AggregateOperatorCode *aggrOperator) {
  std::cout << "[DBG] set aggregate operator" << std::endl;
  m_aggrOperator = aggrOperator;
}

bool ResultHandler::containsFragmentedWindows() {
  return m_hasWindowFragments;
}

void ResultHandler::restorePtrs(int taskId) {
  if (!m_hasRestored) {
    int taskIdx = ((taskId - 1) % m_numberOfSlots);
    m_nextToForward.store(taskIdx);
    m_nextToAggregate.store(taskIdx);
    m_hasRestored = true;
  }
}

ResultHandler::~ResultHandler() = default;

std::vector<PartialResultSlotWithoutFragments> &ResultHandler::getPartialsWithoutFrags() {
  std::cout << "warning: use this function only for testing" << std::endl;
  return m_resultsWithoutFrags;
}

std::vector<PartialResultSlot> &ResultHandler::getPartials() {
  std::cout << "warning: use this function only for testing" << std::endl;
  return m_results;
}

std::vector<PartialWindowResultSlot> &ResultHandler::getWindowPartials() {
  std::cout << "warning: use this function only for testing" << std::endl;
  return m_windowResults;
}