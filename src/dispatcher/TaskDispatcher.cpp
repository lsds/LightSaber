#include "TaskDispatcher.h"

#include <xmmintrin.h>

#include "buffers/UnboundedQueryBuffer.h"

#if defined(HAVE_NUMA)
#include "buffers/PersistentNumaCircularQueryBuffer.h"
#else
#include "buffers/PersistentCircularQueryBuffer.h"
#endif
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "result/ResultHandler.h"
#include "tasks/Task.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Query.h"
#include "utils/QueryConfig.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"
#include "utils/WindowDefinition.h"

TaskDispatcher::TaskDispatcher(Query &query, QueryBuffer &buffer,
                               bool replayTimestamps, bool triggerCheckpoints)
    : ITaskDispatcher(query, triggerCheckpoints),
      m_buffer(buffer),
      m_window(query.getWindowDefinition()),
      m_schema(query.getSchema()),
      m_batchSize(query.getConfig() ? query.getConfig()->getBatchSize() : SystemConf::getInstance().BATCH_SIZE),
      m_tupleSize(m_schema->getTupleSize()),
      m_nextTask(1),
      m_mask(buffer.getMask()),
      m_latencyMark(-1),
      m_thisBatchStartPointer(0),
      m_nextBatchEndPointer(m_batchSize),
      m_replayTimestamps(replayTimestamps)
#if defined(HAVE_SHARED)
      ,
      m_segment(std::make_unique<boost::interprocess::managed_shared_memory>(
          boost::interprocess::open_only, "MySharedMemory"))
#endif
{

  if (replayTimestamps) {
    m_replayBarrier = query.getConfig() ?
        (query.getConfig()->getBundleSize() / query.getConfig()->getBatchSize()) :
        (SystemConf::getInstance().BUNDLE_SIZE / SystemConf::getInstance().BATCH_SIZE);
    if (m_replayBarrier == 0)
      throw std::runtime_error(
          "error: the bundle size should be greater or equal to the batch size "
          "when replaying data with range-based windows");
  }
#if defined(HAVE_NUMA)
  if (PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer)) {
#else
  if (PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer)) {
#endif
    m_parallelInsertion = true;
  }
#if defined(HAVE_SHARED)
  m_o = m_segment->find<long>("offset").first;
  m_s = m_segment->find<long>("step").first;
  m_offset = *m_o;
  m_step = *m_s;
#endif
}

void TaskDispatcher::dispatch(char *data, int length, long latencyMark, long retainMark) {
#if defined(HAVE_SHARED)
  /* Check if we try to recover from existing data */
  if (m_startingFromRecovery) {
    if (m_buffer.getUnsafeRemainingBytes() != 0) {
      m_accumulated = m_buffer.getUnsafeStartPointer();
      m_thisBatchStartPointer = m_buffer.getUnsafeStartPointer();
      m_nextBatchEndPointer = m_buffer.getUnsafeStartPointer() + m_batchSize;
      m_watermark = m_buffer.getLong(
                        m_buffer.normalise(m_buffer.getUnsafeStartPointer())) -
                    1;
      m_buffer.fixTimestamps(
          m_buffer.getUnsafeStartPointer(), m_watermark + 1, m_step,
          m_buffer.getUnsafeRemainingBytes());  // todo: fix this
      assemble(m_buffer.normalise(m_buffer.getUnsafeEndPointer()),
               m_buffer.getUnsafeRemainingBytes());
    }
    m_startingFromRecovery = false;
  }
#endif

  if (!m_parallelInsertion) {
    long idx;
    while ((idx = m_buffer.put(data, length, latencyMark)) < 0) {
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    assemble(idx, length);
  } else {
#if defined(HAVE_NUMA)
    PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
    PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
    long idx; bool ready = false;
    while ((idx = m_buffer.put(data, length, latencyMark, retainMark)) < 0) {
      while (b->tryConsumeNextSlot(idx, length)) {
        assemble(idx, length);
      }
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    while (b->tryConsumeNextSlot(idx, length)) {
      assemble(idx, length);
    }
  }
}

void TaskDispatcher::dispatch(std::shared_ptr<UnboundedQueryBuffer> &data, long latencyMark, long retainMark) {
  if (!m_parallelInsertion) {
    long idx;
    while ((idx = m_buffer.put(data, latencyMark)) < 0) {
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    assemble(idx, data->getBuffer().size());
  } else {
#if defined(HAVE_NUMA)
    PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
    PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
    long idx; bool ready = false;
    int length = data->getBuffer().size();
    while ((idx = m_buffer.put(data, latencyMark, retainMark)) < 0) {
      while (b->tryConsumeNextSlot(idx, length)) {
        assemble(idx, length);
      }
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    while (b->tryConsumeNextSlot(idx, length)) {
      assemble(idx, length);
    }
  }
}

void TaskDispatcher::dispatch(void *data, int length, long latencyMark, long retainMark) {
  if (!m_parallelInsertion) {
    long idx;
    while ((idx = m_buffer.put(data, latencyMark)) < 0) {
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    assemble(idx, length);
  } else {
#if defined(HAVE_NUMA)
    PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
    PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
    long idx; bool ready = false;
#if defined(RDMA_INPUT)
    while ((idx = m_buffer.put(data, latencyMark, retainMark)) < 0) {
      while (b->tryConsumeNextSlot(idx, length)) {
        assemble(idx, length);
      }
      _mm_pause();
      // std::cout << "Failed to dispatch..." << std::endl;
      tryCreateNonProcessingTasks();
    }
    while (b->tryConsumeNextSlot(idx, length)) {
      assemble(idx, length);
    }
#else
    throw std::runtime_error("error: enable RDMA_INPUT");
#endif
  }
}

void TaskDispatcher::dispatchToFirstStream(char *data, int length, long latencyMark) {
  dispatch(data, length, latencyMark);
}

void TaskDispatcher::dispatchToSecondStream(char *data, int length, long latencyMark) {
  throw std::runtime_error("error: dispatching to the second stream is not supported by this dispatcher");
}

bool TaskDispatcher::tryDispatchOrCreateTask(char *data, int length, long latencyMark, long retain, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_buffer.put(data, length, latencyMark, retain, graph)) < 0) {
    tryCreateNonProcessingTasks();
    return false;
  }
  assemble(idx, length);
  return true;
}

bool TaskDispatcher::tryDispatch(char *data, int length, long latencyMark, long retain, std::shared_ptr<LineageGraph> graph) {
  if (!m_parallelInsertion) {
    long idx;

    if ((idx = m_buffer.put(data, length, latencyMark, -1, graph)) < 0) {
      return false;
    }
    assemble(idx, length);
    return true;
  } else {
    long idx;
#if defined(HAVE_NUMA)
    PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
    PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
    if ((idx = m_buffer.put(data, length, latencyMark, retain, graph)) < 0) {
      return false;
    }
    while (b->tryConsumeNextSlot(idx, length)) {
      assemble(idx, length);
    }
    return true;
  }
}

bool TaskDispatcher::tryDispatchToFirstStream (char *data, int length, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  return tryDispatch(data, length, latencyMark, -1, graph);
}

bool TaskDispatcher::tryDispatchToSecondStream(char *data, int length, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  throw std::runtime_error("error: dispatching to the second stream is not supported by this dispatcher");
}

bool TaskDispatcher::tryDispatchSerialToFirstStream(char *data, int length, size_t id, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  throw std::runtime_error("error: this operations is not supported by the task dispatcher");
}

bool TaskDispatcher::tryDispatchSerialToSecondStream(char *data, int length, size_t id, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  throw std::runtime_error("error: dispatching to the second stream is not supported by this dispatcher");
}

void TaskDispatcher::tryToConsume() {
  if (!m_buffer.isPersistent())
    return;

#if defined(HAVE_NUMA)
  PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
  PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
  long idx; bool ready = false; int length;
  while(b->getEmptySlots() < b->getNumberOfSlots()*0.75) {
    while (b->tryConsumeNextSlot(idx, length, true)) {
      assemble(idx, length);
    }
  }
}

void TaskDispatcher::recover() {
#if defined(HAVE_NUMA)
  PersistentNumaCircularQueryBuffer *b = dynamic_cast<PersistentNumaCircularQueryBuffer *>(&m_buffer);
#else
  PersistentCircularQueryBuffer *b = dynamic_cast<PersistentCircularQueryBuffer *>(&m_buffer);
#endif
  long idx; int length; bool ready = false;
  while ((idx = m_buffer.recover(length)) == 0) {
    while (b->tryConsumeNextSlot(idx, length, true)) {
      assemble(idx, length);
    }
    _mm_pause();
    // std::cout << "Failed to dispatch..." << std::endl;
  }
  while (b->getRemainingSlotsToFree() != 0) {
    while (b->tryConsumeNextSlot(idx, length, true)) {
      assemble(idx, length);
    }
  }
}

QueryBuffer *TaskDispatcher::getBuffer() { return &m_buffer; }

QueryBuffer *TaskDispatcher::getFirstBuffer() { return &m_buffer; }

QueryBuffer *TaskDispatcher::getSecondBuffer() {
  throw std::runtime_error(
      "error: getting a second buffer is not supported by this dispatcher");
}

void TaskDispatcher::setTaskQueue(std::shared_ptr<TaskQueue> queue) {
  m_workerQueue = queue;
}

long TaskDispatcher::getBytesGenerated() {
  return m_parent.getBytesGenerated();
}

void TaskDispatcher::setLastTaskId(int taskId) { m_lastTaskId = taskId; }

void TaskDispatcher::setCheckpointCoordinator(
    FileBackedCheckpointCoordinator *coordinator) {
  if (!coordinator) throw std::runtime_error("error: null coordinator pointer");
  if (!m_triggerCheckpoints) return;

  m_coordinator = coordinator;

    m_coordinationTimerThread = std::thread([&]() {
      //Utils::bindProcess(SystemConf::getInstance().WORKER_THREADS+1);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      auto t1 = std::chrono::high_resolution_clock::now();
      auto t2 = t1;
      while (m_triggerCheckpoints) {
        t1 = std::chrono::high_resolution_clock::now();
        while (!m_checkpointFinished) {
          _mm_pause();
        }
        m_checkpointFinished.store(false);
        auto lastTaskId = m_nextTask.load();
        setLastTaskId(lastTaskId);
        m_coordinator->signalWaitCondition();
        // change the type of tasks here?
        t2 = std::chrono::high_resolution_clock::now();
        auto time_span =
            std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        if (time_span.count()*1000 < SystemConf::getInstance().CHECKPOINT_INTERVAL) {
          auto duration = SystemConf::getInstance().CHECKPOINT_INTERVAL -
              (size_t) (time_span.count() * 1000);
          std::this_thread::sleep_for(std::chrono::milliseconds(duration));
        }
      }
    });
    m_coordinationTimerThread.detach();

}

TaskDispatcher::~TaskDispatcher() {
  m_triggerCheckpoints = false;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void TaskDispatcher::assemble(long index, int length) {
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark < 0) {
      // latencyMark = index;
      // get latency mark
      auto systemTimestamp = getSystemTimestamp(index);
      m_latencyMark = systemTimestamp;
      // reset the correct timestamp for the first tuple
      auto tupleTimestamp = getTimestamp(index);
      setTimestamp((int)index, tupleTimestamp);
    }
  }

  m_accumulated += (length);
  if ((!m_window.isRangeBased()) && (!m_window.isRowBased())) {
    throw std::runtime_error(
        "error: window is neither row-based nor range-based");
  }

  while (m_accumulated >= m_nextBatchEndPointer) {
#if defined(HAVE_NUMA)
    m_f = m_nextBatchEndPointer % m_mask;
#else
    m_f = m_nextBatchEndPointer & m_mask;
#endif
    m_f = (m_f == 0) ? m_buffer.getCapacity() : m_f;
    m_f--;
    /* Launch task */
    newTaskFor(
#if defined(HAVE_NUMA)
        m_thisBatchStartPointer % m_mask, m_nextBatchEndPointer % m_mask,
#else
        m_thisBatchStartPointer & m_mask, m_nextBatchEndPointer & m_mask,
#endif
        m_f, m_thisBatchStartPointer, m_nextBatchEndPointer);

    m_thisBatchStartPointer += m_batchSize;
    m_nextBatchEndPointer += m_batchSize;
  }
}

void TaskDispatcher::newTaskFor(long p, long q, long free, long b_, long _d) {
  int taskId = getTaskNumber();
  long size = 0;
  size = (q <= p) ? (q + m_buffer.getCapacity()) - p : q - p;
  if (m_debug) {
    std::cout << "[DBG] Query " + std::to_string(m_parent.getId()) + " task " +
                     std::to_string(taskId) + " [" + std::to_string(p) + ", " +
                     std::to_string(q) + "), free " + std::to_string(free) +
                     ", [" + std::to_string(b_) + ", " + std::to_string(_d) +
                     "] size " + std::to_string(size)
              << std::endl;
  }
  if (q <= p) {
    q += m_buffer.getCapacity();
  }

  /* Find latency mark */
  long mark = -1;
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark >= 0) {
      mark = m_latencyMark;
      m_latencyMark = -1;
    }
  }

  /* Update free pointer */
  free -= m_schema->getTupleSize();
  if (free < 0) {
    std::cout << "error: negative free pointer (" + std::to_string(free) +
                     ") for query " + std::to_string(m_parent.getId());
    exit(1);
  }

  auto batch = WindowBatchFactory::getInstance().newInstance(
      m_batchSize, taskId, free, -1, &m_parent, &m_buffer, &m_window,
      m_schema, mark);

  if (m_window.isRangeBased()) {
    long startTime = getTimestamp((int)(p));
    long endTime = getTimestamp((int)(q - m_tupleSize));
    batch->setBatchTimestamps(startTime, endTime);
    if (m_replayTimestamps) {
      long prevStartTime = 0;
      long prevEndTime = 0;
      if (m_step != -1) {
        prevStartTime = (p != 0)
                            ? getTimestamp((int)(p - m_batchSize - m_tupleSize))
                            : getTimestamp((int)(m_buffer.getCapacity() -
                                                 m_batchSize - m_tupleSize));
        prevEndTime =
            (p != 0)
                ? getTimestamp((int)(p - m_tupleSize))
                : getTimestamp((int)(m_buffer.getCapacity() - m_tupleSize));

        if (startTime + m_offset - prevEndTime >= m_step && m_offset > 0 &&
            !SystemConf::getInstance().RECOVER) {  // sanity check
          m_offset -= m_step;
          if (m_buffer.isPersistent()) {
            m_buffer.updateStepAndOffset(m_step, m_offset);
          }
        }
      }
      setTimestamp((int)(p), startTime + m_offset);
      setTimestamp((int)(q - m_tupleSize), endTime + m_offset);
      batch->setBatchTimestamps(startTime + m_offset, endTime + m_offset);
      batch->setPrevTimestamps(prevStartTime, prevEndTime);
#if defined(HAVE_OoO)
      auto tmpWatermark = startTime + m_offset - 1;
      if (taskId % m_watermarkFrequency == 0 && tmpWatermark > m_watermark) {
        m_watermark = tmpWatermark;
        // std::cout << "[DBG] " << taskId << " taskId " << m_watermark << "
        // watermark changed " << std::endl;
      }
      batch->setWatermark(m_watermark);
#endif
      if (m_debug) {
        std::cout << "[DBG] " << taskId << " taskId " << m_step << " step "
                  << m_offset << " offset " << (p + m_recoveryOffset)
                  << " startIdx " << (q + m_recoveryOffset)
                  << " endIdx " << prevEndTime << " prevEndTime " << startTime
                  << " initialStartTime " << endTime << " initialEndTime "
                  << getTimestamp((int)(p)) << " startTime "
                  << getTimestamp((int)(q - m_tupleSize)) << " endTime "
                  << std::endl;
      }
      batch->setTimestampOffset(m_offset);
      if (taskId % (m_replayBarrier) == 0) {
        if (m_step == -1) {
          m_step = endTime + 1;
          if (m_buffer.isPersistent()) {
            m_buffer.updateStepAndOffset(m_step, m_offset);
          }
        }

        if (_d <= (long)m_buffer.getCapacity()) { // &&
            //!SystemConf::getInstance().RECOVER) {  // SystemConf::getInstance().CIRCULAR_BUFFER_SIZE)
          m_offset += m_step;
          if (m_buffer.isPersistent()) {
            m_buffer.updateStepAndOffset(m_step, m_offset);
          }
        }
        // batch->setBatchTimestamps(startTime, endTime+offset);
      }
#if defined(HAVE_SHARED)
      *m_o = m_offset;
      *m_s = m_step;
#endif
    } else {
      long prevStartTime = 0;
      long prevEndTime = 0;
      if (m_step != -1) {
        prevStartTime = (p != 0)
                        ? getTimestamp((int)(p - m_batchSize - m_tupleSize))
                        : getTimestamp((int)(m_buffer.getCapacity() -
                m_batchSize - m_tupleSize));
        prevEndTime =
            (p != 0)
            ? getTimestamp((int)(p - m_tupleSize))
            : getTimestamp((int)(m_buffer.getCapacity() - m_tupleSize));
      } else {
        m_step = 0;
      }
      batch->setPrevTimestamps(prevStartTime, prevEndTime);
    }
  } else {
    batch->setBatchTimestamps(-1, -1);
  }

  batch->setBufferPointers((int)p, (int)q);
  batch->setStreamPointers(b_, _d);

  TaskType type = (m_createMergeTasks.load()) ? TaskType::PROCESS : TaskType::ONLY_PROCESS;
  batch->setTaskType(type);

  if (SystemConf::getInstance().LINEAGE_ON) {
    auto slotId = p / batch->getBatchSize(); //batch->getBufferStartPointer() / batch->getBatchSize();
    auto &slot = m_buffer.getSlots()[slotId];
    auto graph = slot.getLineageGraph();
    if (!graph) {
      /*throw std::runtime_error(
          "error: the lineage graph is not initialized in task dispatcher " +
          std::to_string(m_parent.getId()) + " for slot " +
          std::to_string(slot.m_id) + " with start pointer " +
          std::to_string(batch->getBufferStartPointer()));*/
      graph = LineageGraphFactory::getInstance().newInstance();
    }
    batch->setLineageGraph(graph);
    graph.reset();
    if (m_buffer.isPersistent()) {
      auto fp1 = batch->getFreePointer();
      auto fo1 = m_nextBatchEndPointer + m_recoveryOffset;
      if (m_debug) {
        std::cout << "[DBG] " << taskId << " taskId " << m_buffer.getBufferId()
                  << " bufferId setting freePtr1 " << fp1
                  << " freeOffset1 " << fo1
                  << " in slot " << (fp1/m_buffer.getCapacity())%10
                  << " for query " << m_parent.getId()
                  << " with ptr " << batch->getLineageGraph()->m_graph[m_parent.getId()].get() << std::endl;
      }
      batch->getLineageGraph()->m_graph[m_parent.getId()]->m_freePtr1 = fp1;
      batch->getLineageGraph()->m_graph[m_parent.getId()]->m_freeOffset1 = fo1;
      batch->getLineageGraph()->m_isValid = true;
    }
  }

  auto task = TaskFactory::getInstance().newInstance(taskId, batch, nullptr, type);

  while (!m_workerQueue->try_enqueue(task)) {
    /*std::cout << "warning: waiting to enqueue PROCESS task in the task dispatcher "
              << std::to_string(m_parent.getId()) << " with size "
              << std::to_string(m_workerQueue->size_approx()) << std::endl;*/
  }

  if (m_checkpointCounter > 0)
    m_checkpointCounter--;
}

int TaskDispatcher::getTaskNumber() {
  int id = m_nextTask.fetch_add(1);
  if (m_nextTask.load() == INT_MAX) m_nextTask.store(1);
  return id;
}

void TaskDispatcher::setTaskNumber(int taskId) {
  m_nextTask.store(taskId);
  m_parent.getResultHandler()->restorePtrs(taskId);
}

void TaskDispatcher::setStepAndOffset(long step, long offset) {
  m_step = step;
  m_recoveryOffset = offset;
  //m_accumulated = offset;
  //m_thisBatchStartPointer = offset;
  //m_nextBatchEndPointer += offset;
  //m_offset = offset;
}

void TaskDispatcher::createMergeTasks(bool flag) {
  m_createMergeTasks.store(flag);
}

void TaskDispatcher::tryCreateNonProcessingTasks() {
  if (m_workerQueue->size_approx() >= m_parent.getTaskQueueCapacity()) {
    return;
  }

  // create a merge task
  bool flag = (SystemConf::getInstance().CREATE_MERGE_WITH_CHECKPOINTS && m_createMergeTasks.load()) ||
      !SystemConf::getInstance().CREATE_MERGE_WITH_CHECKPOINTS;
  bool nextQueryJoin = false; //m_parent.getDownstreamQuery() != nullptr ? m_parent.getDownstreamQuery()->getNumberOfUpstreamQueries() == 2 : false;
  if (((int)m_workerQueue->size_approx() < SystemConf::getInstance().WORKER_THREADS && flag) || nextQueryJoin) {
      //&& m_createMergeTasks.load()) {
    //if (m_createMergeTasks.load())
    //  std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, &m_parent, nullptr, &m_window, m_schema, -1);
    auto type = TaskType::MERGE_FORWARD; // m_createMergeTasks.load() ? TaskType::MERGE : TaskType::MERGE_FORWARD;
    batch->setTaskType(type);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, type);
    while (!m_workerQueue->try_enqueue(task)) {
      if (m_workerQueue->size_approx() >= (0.75 * m_parent.getTaskQueueCapacity())) {
        std::cout << "warning: waiting to enqueue MERGE_FORWARD task in the task dispatcher "
                  << std::to_string(m_parent.getId()) << " with size "
                  << std::to_string(m_workerQueue->size_approx()) << std::endl;
        WindowBatchFactory::getInstance().free(batch);
        TaskFactory::getInstance().free(task);
      }
    }
  }
  // create a checkpoint task
  if (m_triggerCheckpoints && m_coordinator && m_coordinator->hasWorkUnsafe(m_parent.getId()) && m_checkpointCounter < SystemConf::getInstance().WORKER_THREADS) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, &m_parent, nullptr, &m_window, m_schema, -1);
    batch->setTaskType(TaskType::CHECKPOINT);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, TaskType::CHECKPOINT);
    if (!m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue CHECKPOINT task in the task dispatcher "
                << std::to_string(m_parent.getId()) << " with size "
                << std::to_string(m_workerQueue->size_approx()) << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    } else {
      m_checkpointCounter++;
    }
  }
}

long TaskDispatcher::getTimestamp(int index) {
  // wrap around if it gets out of bounds
  if (index < 0) index = m_buffer.getCapacity() + index;
  long value = m_buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long)Utils::getTupleTimestamp(value);
  else
    return value;
}

long TaskDispatcher::getSystemTimestamp(int index) {
  // wrap around if it gets out of bounds
  if (index < 0) index = m_buffer.getCapacity() + index;
  long value = m_buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long)Utils::getSystemTimestamp(value);
  else
    return value;
}

void TaskDispatcher::setTimestamp(int index, long timestamp) {
  // wrap around if it gets out of bounds
  if (index < 0) index = m_buffer.getCapacity() + index;
  m_buffer.setLong(index, timestamp);
}