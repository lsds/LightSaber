#include "JoinTaskDispatcher.h"

#include <xmmintrin.h>

#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "tasks/Task.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/Query.h"
#include "utils/QueryConfig.h"
#include "utils/QueryOperator.h"
#include "utils/TupleSchema.h"
#include "utils/Utils.h"
#include "utils/WindowDefinition.h"

JoinTaskDispatcher::JoinTaskDispatcher(Query &query, QueryBuffer &firstBuffer,
                                       QueryBuffer &secondBuffer, bool replayTimestamps, bool triggerCheckpoints)
    : ITaskDispatcher(query, triggerCheckpoints),
      m_firstBuffer(firstBuffer),
      m_secondBuffer(secondBuffer),
      m_firstWindow(query.getFirstWindowDefinition()),
      m_secondWindow(query.getSecondWindowDefinition()),
      m_firstSchema(query.getFirstSchema()),
      m_secondSchema(query.getSecondSchema()),
      m_batchSize(query.getConfig() ? query.getConfig()->getBatchSize() : SystemConf::getInstance().BATCH_SIZE),
      m_firstTupleSize(m_firstSchema->getTupleSize()),
      m_secondTupleSize(m_secondSchema->getTupleSize()),
      m_nextTask(1),
      m_firstEndIndex(-m_firstTupleSize),
      m_firstLastEndIndex(-m_firstTupleSize),
      m_secondLastEndIndex(-m_secondTupleSize),
      m_mask(m_firstBuffer.getMask()),
      m_latencyMark(-1),
      m_leftList(std::make_unique<CircularTaskList>(128)),
      m_rightList(std::make_unique<CircularTaskList>(128)) {

  if (m_firstBuffer.getCapacity() != m_secondBuffer.getCapacity())
    throw std::runtime_error(
        "error: both first and second buffer have to be the same size for the join dispatcher");

  if (!(!(m_firstTupleSize == 0) &&
       !(m_firstTupleSize & (m_firstTupleSize - 1))) ||
      !(!(m_secondTupleSize == 0) &&
       !(m_secondTupleSize & (m_secondTupleSize - 1))))
    throw std::runtime_error(
        "error: both first and second tuple sizes have to be a power of two");

  if (replayTimestamps) {
    m_replayBarrier = query.getConfig() ? (query.getConfig()->getBundleSize() / query.getConfig()->getBatchSize()) :
                      (SystemConf::getInstance().BUNDLE_SIZE / SystemConf::getInstance().BATCH_SIZE);
    if (m_replayBarrier == 0)
      throw std::runtime_error(
          "error: the bundle size should be greater or equal to the batch size "
          "when replaying data with range-based windows");
  }
}

void JoinTaskDispatcher::dispatch(char *data, int length, long latencyMark, long retainMark) {
  long idx;
  while ((idx = m_firstBuffer.put(data, length, latencyMark)) < 0) {
    _mm_pause();
    //std::cout << "Failed to dispatch..." << std::endl;
    //tryCreateNonProcessingTasks();
  }
  assembleFirst(idx, length);
}

void JoinTaskDispatcher::dispatch(std::shared_ptr<UnboundedQueryBuffer> &data, long latencyMark, long retainMark) {
  throw std::runtime_error("error: dispatch with UnboundedQueryBuffer is not implemented");
}

void JoinTaskDispatcher::dispatchToFirstStream(char *data, int length, long latencyMark) {
  dispatch(data, length, latencyMark);
}

void JoinTaskDispatcher::dispatchToSecondStream(char *data, int length, long latencyMark) {
  long idx;
  while ((idx = m_secondBuffer.put(data, length, latencyMark)) < 0) {
    _mm_pause();
    //std::cout << "Failed to dispatch..." << std::endl;
    //tryCreateNonProcessingTasks();
  }
  assembleSecond(idx, length);
}

bool JoinTaskDispatcher::tryDispatchOrCreateTask(char *data, int length, long latencyMark, long retain, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_firstBuffer.put(data, length, latencyMark, -1, graph)) < 0) {
    //tryCreateNonProcessingTasks();
    return false;
  }
  assembleFirst(idx, length);
  return true;
}

bool JoinTaskDispatcher::tryDispatch(char *data, int length, long latencyMark, long retain, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_firstBuffer.put(data, length, latencyMark, -1, graph)) < 0) {
    return false;
  }
  assembleFirst(idx, length);
  return true;
}

bool JoinTaskDispatcher::tryDispatchToFirstStream (char *data, int length, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  return tryDispatch(data, length, latencyMark, -1, graph);
}

bool JoinTaskDispatcher::tryDispatchToSecondStream(char *data, int length, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_secondBuffer.put(data, length, latencyMark, -1, graph)) < 0) {
    return false;
  }
  assembleSecond(idx, length);
  return true;
}

bool JoinTaskDispatcher::tryDispatchSerialToFirstStream(char *data, int length, size_t id, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_firstBuffer.put(data, length, latencyMark, -1, graph)) < 0) {
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(m_left);
    m_leftList->push_back(id, idx, length);
  }
  tryAssembleTask();
  return true;
}

bool JoinTaskDispatcher::tryDispatchSerialToSecondStream(char *data, int length, size_t id, long latencyMark, std::shared_ptr<LineageGraph> graph) {
  long idx;
  if ((idx = m_secondBuffer.put(data, length, latencyMark, -1, graph)) < 0) {
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(m_right);
    m_rightList->push_back(id, idx, length);
  }
  tryAssembleTask();
  return true;
}

void JoinTaskDispatcher::tryToConsume() {
  throw std::runtime_error("error: the recover function is not implemented");
}

void JoinTaskDispatcher::recover() {
  throw std::runtime_error("error: the recover function is not implemented");
}

void JoinTaskDispatcher::tryAssembleTask() {
  {
    std::lock_guard<std::mutex> lock(m_left);
    while (m_leftList->size() > 0) {
      auto t = m_leftList->front();
      if (t != nullptr && t->m_id <= m_assembleId) {
        assembleFirst(t->m_idx, t->m_length);
        m_leftList->pop_front();
      } else {
        break;
      }
    }
  }
  {
    std::lock_guard<std::mutex> lock(m_right);
    while (m_rightList->size() > 0) {
      auto t = m_rightList->front();
      if (t != nullptr && t->m_id <= m_assembleId) {
        assembleSecond(t->m_idx, t->m_length);
        m_rightList->pop_front();
      } else if (t != nullptr && t->m_id > m_assembleId) {
        m_assembleId++;
        break;
      } else {
        break;
      }
    }
  }
}

QueryBuffer *JoinTaskDispatcher::getBuffer() { return &m_firstBuffer; }

QueryBuffer *JoinTaskDispatcher::getFirstBuffer() { return &m_firstBuffer; }

QueryBuffer *JoinTaskDispatcher::getSecondBuffer() {return &m_secondBuffer; }

void JoinTaskDispatcher::setTaskQueue(std::shared_ptr<TaskQueue> queue) {
  m_workerQueue = queue;
}

long JoinTaskDispatcher::getBytesGenerated() {
  return m_parent.getBytesGenerated();
}

void JoinTaskDispatcher::setLastTaskId(int taskId) { m_lastTaskId = taskId; }

void JoinTaskDispatcher::setCheckpointCoordinator(
    FileBackedCheckpointCoordinator *coordinator) {
  (void*) coordinator;
  throw std::runtime_error("error: setting a coordinator is not supported yet");
}

JoinTaskDispatcher::~JoinTaskDispatcher() {
  m_triggerCheckpoints = false;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void JoinTaskDispatcher::assembleFirst(long idx, int length) {
  if (m_debug) {
    std::cout << "[DBG] assemble 1: idx "  + std::to_string(idx) +
                     " length " + std::to_string(length) << std::endl;
  }
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark < 0) {
      // latencyMark = index;
      // get latency mark
      auto systemTimestamp = getSystemTimestamp(m_firstBuffer, idx);
      m_latencyMark = systemTimestamp;
      // reset the correct timestamp for the first tuple
      auto tupleTimestamp = getTimestamp(m_firstBuffer, idx);
      setTimestamp(m_firstBuffer, (int)idx, tupleTimestamp);
    }
  }

  m_firstEndIndex = idx + length - m_firstTupleSize;
  {
    std::lock_guard<std::mutex> lock(m_lock);
    if (m_firstEndIndex < m_firstStartIndex)
      m_firstEndIndex += m_firstBuffer.getCapacity();

    m_firstToProcessCount =
        (m_firstEndIndex - m_firstStartIndex + m_firstTupleSize) /
        m_firstTupleSize;

    /*
     * Check whether we have to move the pointer that indicates the oldest
     * window in this buffer that has not yet been closed. If we grab the data
     * to create a task, the start pointer will be set to this next pointer.
     *
     */
    if (m_firstWindow.isRowBased()) {
      while ((m_firstNextIndex + m_firstWindow.getSize() * m_firstTupleSize) <
             m_firstEndIndex) {
        m_firstNextIndex += m_firstTupleSize * m_firstWindow.getSlide();
      }
    } else if (m_firstWindow.isRangeBased()) {
      m_firstNextTime = getTimestamp(m_firstBuffer, m_firstNextIndex);
      m_firstEndTime = getTimestamp(m_firstBuffer, m_firstEndIndex);

      while ((m_firstNextTime + m_firstWindow.getSize()) < m_firstEndTime && m_firstNextIndex < m_firstEndIndex) {
        m_firstNextIndex += m_firstTupleSize;
        m_firstNextTime = getTimestamp(m_firstBuffer, m_firstNextIndex);
      }

    } else {
      throw std::runtime_error(
          "error: window is neither row-based nor range-based");
    }

    /* Check whether we have enough data to create a task */
    int size = (m_firstToProcessCount * m_firstTupleSize) +
               (m_secondToProcessCount * m_secondTupleSize);

    if (!m_symmetric) {
      if (size >= m_batchSize && m_firstToProcessCount > 0 && m_secondToProcessCount > 0)
        createTask(true);
    } else {
      if (size >= m_batchSize && m_firstToProcessCount > m_firstWindow.getSlide() && m_secondToProcessCount > m_secondWindow.getSlide())
        createSymmetricTask(true);
    }
  }
  if (m_debug) {
    std::cout << "[DBG] finishing assemble 1: idx "  + std::to_string(idx) +
        " length " + std::to_string(length) << std::endl;
  }
}

void JoinTaskDispatcher::assembleSecond(long idx, int length) {
  if (m_debug) {
    std::cout << "[DBG] assemble 2: idx "  + std::to_string(idx) +
        " length " + std::to_string(length) << std::endl;
  }

  m_secondEndIndex = idx + length - m_secondTupleSize;

  {
    std::lock_guard<std::mutex> lock(m_lock);
    if (m_secondEndIndex < m_secondStartIndex)
      m_secondEndIndex += m_secondBuffer.getCapacity();

    m_secondToProcessCount =
        (m_secondEndIndex - m_secondStartIndex + m_secondTupleSize) /
        m_secondTupleSize;

    if (m_secondWindow.isRowBased()) {
      while ((m_secondNextIndex + m_secondWindow.getSize() *
                                      m_secondTupleSize) < m_secondEndIndex) {
        m_secondNextIndex += m_secondTupleSize * m_secondWindow.getSlide();
      }

    } else if (m_secondWindow.isRangeBased()) {
      m_secondNextTime = getTimestamp(m_secondBuffer, m_secondNextIndex);
      m_secondEndTime = getTimestamp(m_secondBuffer, m_secondEndIndex);

      while ((m_secondNextTime + m_secondWindow.getSize()) < m_secondEndTime && m_secondNextIndex < m_secondEndIndex) {
        m_secondNextIndex += m_secondTupleSize;
        m_secondNextTime = getTimestamp(m_secondBuffer, m_secondNextIndex);
      }

    } else {
      throw std::runtime_error(
          "error: window is neither row-based nor range-based");
    }

    /* Check whether we have enough data to create a task */
    int size = (m_firstToProcessCount * m_firstTupleSize) +
               (m_secondToProcessCount * m_secondTupleSize);

    if (!m_symmetric) {
      if (size >= m_batchSize && m_firstToProcessCount > 0 && m_secondToProcessCount > 0)
        createTask(false);
    } else {
      if (size >= m_batchSize && m_firstToProcessCount > m_firstWindow.getSlide() && m_secondToProcessCount > m_secondWindow.getSlide())
        createSymmetricTask(false);
    }
  }
  if (m_debug) {
    std::cout << "[DBG] finishing assemble 2: idx "  + std::to_string(idx) +
        " length " + std::to_string(length) << std::endl;
  }
}

void JoinTaskDispatcher::createTask(bool assembledFirst) {
  int taskId = getTaskNumber();
  long firstFreePointer = INT_MIN;
  long secondFreePointer = INT_MIN;

  if (m_firstNextIndex != m_firstStartIndex) {
    firstFreePointer = (m_firstNextIndex - m_firstTupleSize) & m_mask;
    m_prevFirstFreePointer = firstFreePointer;
  }

  if (m_secondNextIndex != m_secondStartIndex) {
    secondFreePointer = (m_secondNextIndex - m_secondTupleSize) & m_mask;
    m_prevSecondFreePointer = secondFreePointer;
  }

  /* Find latency mark */
  int mark = -1;
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark >= 0) {
      mark = m_latencyMark;
      m_latencyMark = -1;
    }
  }

  auto batch1 = WindowBatchFactory::getInstance().newInstance(
      m_batchSize, taskId, firstFreePointer, secondFreePointer, &m_parent,
      &m_firstBuffer, &m_firstWindow, m_firstSchema, mark,
      m_prevFirstFreePointer, m_prevSecondFreePointer);

  auto batch2 = WindowBatchFactory::getInstance().newInstance(
      m_batchSize, taskId, INT_MIN, INT_MIN, &m_parent, &m_secondBuffer,
      &m_secondWindow, m_secondSchema, -1);

  // todo: Fix the buffer pointers. At the moment it works only when inserting small number of tuples
  // from both sides. Set properly the input rate and batch size for optimal performance.
  if (assembledFirst) {
    batch1->setBufferPointers(
        m_firstLastEndIndex + m_firstTupleSize,
        normaliseIndex(m_firstBuffer, m_firstLastEndIndex, m_firstEndIndex));

    batch2->setBufferPointers(
        m_secondStartIndex,
        normaliseIndex(m_secondBuffer, m_secondStartIndex, m_secondEndIndex));
  } else {
    batch1->setBufferPointers(
        m_firstStartIndex,
        normaliseIndex(m_firstBuffer, m_firstStartIndex, m_firstEndIndex));

    batch2->setBufferPointers(
        m_secondLastEndIndex + m_secondTupleSize,
        normaliseIndex(m_secondBuffer, m_secondLastEndIndex, m_secondEndIndex));
  }

  if (SystemConf::getInstance().LINEAGE_ON) {
    auto graph = LineageGraphFactory::getInstance().newInstance();
    batch1->setLineageGraph(graph);
    graph.reset();
    auto slotId1 = batch1->getBufferStartPointer() / batch1->getBatchSize();
    auto &slot1 = m_firstBuffer.getSlots()[slotId1];
    {
      std::lock_guard<std::mutex> l (slot1.m_updateLock);
      if (slot1.m_graph && slot1.m_graph->m_isValid) {
        batch1->getLineageGraph()->mergeGraphs(slot1.m_graph);
        slot1.m_graph.reset();
      } else {
        slot1.m_graph.reset();
      }
    }

    auto slotId2 = batch2->getBufferStartPointer() / batch2->getBatchSize();
    auto &slot2 = m_secondBuffer.getSlots()[slotId2];
    {
      std::lock_guard<std::mutex> l(slot2.m_updateLock);
      if (slot2.m_graph && slot2.m_graph->m_isValid) {
        batch1->getLineageGraph()->mergeGraphs(slot2.m_graph);
        slot2.m_graph.reset();
      } else {
        slot1.m_graph.reset();
      }
    }

    if (batch1->getLineageGraph().use_count() > 1) {
      throw std::runtime_error("error: the lineage graph has multiple owners");
    }

    if (m_firstBuffer.isPersistent() || m_secondBuffer.isPersistent())
      throw std::runtime_error(
          "error: the lineage graph is not supported yet for joins");
  }

  if (m_debug) {
    std::cout << "[DBG] dispatch task " + std::to_string(taskId) +
                     " batch-1 [" +
                     std::to_string(batch1->getBufferStartPointer()) + ", " +
                     std::to_string(batch1->getBufferEndPointer()) +
                     "] batch-2 [" +
                     std::to_string(batch2->getBufferStartPointer()) + ", " +
                     std::to_string(batch2->getBufferEndPointer()) + "]"
              << std::endl;
  }

  m_firstLastEndIndex = m_firstEndIndex;
  m_secondLastEndIndex = m_secondEndIndex;

  TaskType type =
      (m_createMergeTasks.load()) ? TaskType::PROCESS : TaskType::ONLY_PROCESS;
  batch1->setTaskType(type);
  auto task =
      TaskFactory::getInstance().newInstance(taskId, batch1, batch2, type);

  if (SystemConf::getInstance().LINEAGE_ON) {
    m_parent.getOperator()->writeOffsets(taskId, batch1->getFreePointer(), batch2->getFreePointer());
  }

  while (!m_workerQueue->try_enqueue(task)) {
    if (m_debug)
    std::cout << "warning: waiting to enqueue PROCESS task in the join task dispatcher "
                << std::to_string(m_parent.getId())
                << " with size " << std::to_string(m_workerQueue->size_approx()) << std::endl;
  }

  /*
   * First, reduce the number of tuples that are ready for processing by the
   * number of tuples that are fully processed in the task that was just
   * created.
   */
  if (m_firstNextIndex != m_firstStartIndex)
    m_firstToProcessCount -=
        (m_firstNextIndex - m_firstStartIndex) / m_firstTupleSize;

  if (m_secondNextIndex != m_secondStartIndex)
    m_secondToProcessCount -=
        (m_secondNextIndex - m_secondStartIndex) / m_secondTupleSize;

  /*
   * Second, move the start pointer for the next task to the next pointer.
   */
  if (m_firstNextIndex > m_mask)
    m_firstNextIndex = m_firstNextIndex & m_mask;

  if (m_secondNextIndex > m_mask)
    m_secondNextIndex = m_secondNextIndex & m_mask;

  m_firstStartIndex = m_firstNextIndex;
  m_secondStartIndex = m_secondNextIndex;
}

void JoinTaskDispatcher::createSymmetricTask(bool assembledFirst) {
  int taskId = getTaskNumber();
  long firstFreePointer = INT_MIN;
  long secondFreePointer = INT_MIN;

  if (!m_firstWindow.isTumbling() || !m_secondWindow.isTumbling() ||
      !m_firstWindow.isRowBased() || !m_secondWindow.isRowBased())
    throw std::runtime_error("error: these window types are not supported by the join dipatcher");

  /* Find latency mark */
  int mark = -1;
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark >= 0) {
      mark = m_latencyMark;
      m_latencyMark = -1;
    }
  }

  int iterations = m_batchSize / (m_firstWindow.getSlide() * m_firstTupleSize + m_secondWindow.getSlide() * m_secondTupleSize);
  if (iterations == 0)
    iterations = 1;

  if (m_firstToProcessCount < iterations * m_firstWindow.getSlide())
    iterations = m_firstToProcessCount / m_firstWindow.getSlide();

  if (m_secondToProcessCount < iterations * m_secondWindow.getSlide())
    iterations = m_secondToProcessCount / m_secondWindow.getSlide();

  firstFreePointer = (m_firstStartIndex + iterations * m_firstWindow.getSlide() * m_firstTupleSize) & m_mask;
  firstFreePointer = (firstFreePointer == 0) ? m_firstBuffer.getCapacity() : firstFreePointer;
  firstFreePointer--;

  secondFreePointer = (m_secondStartIndex + iterations * m_secondWindow.getSlide() * m_secondTupleSize) & m_mask;
  secondFreePointer = (secondFreePointer == 0) ? m_secondBuffer.getCapacity() : secondFreePointer;
  secondFreePointer--;

  auto batch1 = WindowBatchFactory::getInstance().newInstance(
      m_batchSize, taskId, firstFreePointer, secondFreePointer, &m_parent,
      &m_firstBuffer, &m_firstWindow, m_firstSchema, mark);

  auto batch2 = WindowBatchFactory::getInstance().newInstance(
      m_batchSize, taskId, INT_MIN, INT_MIN, &m_parent, &m_secondBuffer,
      &m_secondWindow, m_secondSchema, -1);


  batch1->setBufferPointers(m_firstStartIndex,
                            normaliseIndex(m_firstBuffer, m_firstStartIndex,
                                           m_firstStartIndex + iterations * m_firstWindow.getSlide() * m_firstTupleSize - m_firstTupleSize));
  batch2->setBufferPointers(m_secondStartIndex,
                            normaliseIndex(m_secondBuffer, m_secondStartIndex,
                                           m_secondStartIndex + iterations * m_secondWindow.getSlide() * m_secondTupleSize - m_secondTupleSize));

  if (SystemConf::getInstance().LINEAGE_ON) {
    throw std::runtime_error("error: lineage not supported yet for symmetric tasks");
  }

  if (m_debug) {
    std::cout << "[DBG] dispatch task " + std::to_string(taskId) +
        " batch-1 [" +
        std::to_string(batch1->getBufferStartPointer()) + ", " +
        std::to_string(batch1->getBufferEndPointer()) +
        "] f[" + std::to_string(batch1->getFreePointer()) + "] batch-2 [" +
        std::to_string(batch2->getBufferStartPointer()) + ", " +
        std::to_string(batch2->getBufferEndPointer()) + "] f[" +
        std::to_string(batch1->getSecondFreePointer()) + "]" << std::endl;
  }

  m_firstLastEndIndex = m_firstEndIndex;
  m_secondLastEndIndex = m_secondEndIndex;

  TaskType type =
      (m_createMergeTasks.load()) ? TaskType::PROCESS : TaskType::ONLY_PROCESS;
  batch1->setTaskType(type);
  auto task =
      TaskFactory::getInstance().newInstance(taskId, batch1, batch2, type);

  while (!m_workerQueue->try_enqueue(task)) {
    if (m_debug)
      std::cout << "warning: waiting to enqueue PROCESS task in the join task dispatcher "
                << std::to_string(m_parent.getId())
                << " with size " << std::to_string(m_workerQueue->size_approx()) << std::endl;
  }

  /*
   * First, reduce the number of tuples that are ready for processing by the
   * number of tuples that are fully processed in the task that was just
   * created.
   */

  m_firstToProcessCount -= iterations * m_firstWindow.getSlide();
  m_secondToProcessCount -= iterations * m_secondWindow.getSlide();

  /*
   * Second, move the start pointer for the next task to the next pointer.
   */

  m_firstStartIndex = (m_firstStartIndex + iterations * m_firstWindow.getSlide() * m_firstTupleSize) & m_mask;
  m_secondStartIndex = (m_secondStartIndex + iterations * m_secondWindow.getSlide() * m_secondTupleSize) & m_mask;
}

int JoinTaskDispatcher::getTaskNumber() {
  int id = m_nextTask.fetch_add(1);
  if (m_nextTask.load() == INT_MAX) m_nextTask.store(1);
  return id;
}

void JoinTaskDispatcher::setTaskNumber(int taskId) {
  throw std::runtime_error("error: the setTaskNumber function is not implemented");
}

void JoinTaskDispatcher::setStepAndOffset(long step, long offset) {
  throw std::runtime_error("error: the setStepAndOffset function is not implemented");
}

void JoinTaskDispatcher::createMergeTasks(bool flag) {
  m_createMergeTasks.store(flag);
}

void JoinTaskDispatcher::tryCreateNonProcessingTasks() {
  if (m_workerQueue->size_approx() >= m_parent.getTaskQueueCapacity()) {
    return;
  }
  // create a merge task
  bool flag = (SystemConf::getInstance().CREATE_MERGE_WITH_CHECKPOINTS && m_createMergeTasks.load()) ||
      !SystemConf::getInstance().CREATE_MERGE_WITH_CHECKPOINTS;
  if ((int)m_workerQueue->size_approx() < SystemConf::getInstance().WORKER_THREADS && flag) {
      //&& m_createMergeTasks.load()) {
    //if (m_createMergeTasks.load())
    //  std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, &m_parent, nullptr, &m_firstWindow, m_firstSchema, -1);
    auto type = TaskType::MERGE_FORWARD; // m_createMergeTasks.load() ? TaskType::MERGE : TaskType::MERGE_FORWARD;
    batch->setTaskType(type);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, type);
    if (!m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue MERGE_FORWARD task in the join task dispatcher "
                << std::to_string(m_parent.getId())
                << " with size " << std::to_string(m_workerQueue->size_approx()) << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    }
  }
  // create a checkpoint task
  if (m_triggerCheckpoints && m_coordinator && m_coordinator->hasWorkUnsafe(m_parent.getId()) && m_checkpointCounter < SystemConf::getInstance().WORKER_THREADS) {
    auto batch = WindowBatchFactory::getInstance().newInstance(
        0, 0, -1, -1, &m_parent, nullptr, &m_firstWindow, m_firstSchema, -1);
    batch->setTaskType(TaskType::CHECKPOINT);
    auto task =
        TaskFactory::getInstance().newInstance(0, batch, nullptr, TaskType::CHECKPOINT);
    if (!m_workerQueue->try_enqueue(task)) {
      std::cout << "warning: waiting to enqueue CHECKPOINT task in the join task dispatcher "
                << std::to_string(m_parent.getId())
                << " with size " << std::to_string(m_workerQueue->size_approx()) << std::endl;
      WindowBatchFactory::getInstance().free(batch);
      TaskFactory::getInstance().free(task);
    } else {
      m_checkpointCounter++;
    }
  }
}

long JoinTaskDispatcher::normaliseIndex(QueryBuffer &buffer, long p, long q) {
  if (q < p)
    return (q + buffer.getCapacity());
  return q;
}

long JoinTaskDispatcher::getTimestamp(QueryBuffer &buffer, int index) {
  // wrap around if it gets out of bounds
  if (index < 0) index = buffer.getCapacity() + index;
  long value = buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long)Utils::getTupleTimestamp(value);
  else
    return value;
}

long JoinTaskDispatcher::getSystemTimestamp(QueryBuffer &buffer, int index) {
  // wrap around if it gets out of bounds
  if (index < 0) index = buffer.getCapacity() + index;
  long value = buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long)Utils::getSystemTimestamp(value);
  else
    return value;
}

void JoinTaskDispatcher::setTimestamp(QueryBuffer &buffer, int index, long timestamp) {
  // wrap around if it gets out of bounds
  if (index < 0) index = buffer.getCapacity() + index;
  buffer.setLong(index, timestamp);
}