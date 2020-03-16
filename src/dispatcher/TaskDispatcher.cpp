#include "TaskDispatcher.h"
#include "tasks/Task.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/WindowDefinition.h"
#include "utils/TupleSchema.h"
#include "utils/Query.h"
#include "utils/Utils.h"
#include "result/ResultHandler.h"


TaskDispatcher::TaskDispatcher(Query &query, QueryBuffer &buffer, bool replayTimestamps)
    : m_workerQueue(query.getTaskQueue()),
      m_parent(query),
      m_buffer(buffer),
      m_window(query.getWindowDefinition()),
      m_schema(query.getSchema()),
      m_batchSize(SystemConf::getInstance().BATCH_SIZE),
      m_tupleSize(m_schema->getTupleSize()),
      m_nextTask(1),
      m_mask(buffer.getMask()),
      m_latencyMark(-1),
      m_thisBatchStartPointer(0),
      m_nextBatchEndPointer(m_batchSize),
      m_replayTimestamps(replayTimestamps) {

  if (replayTimestamps) {
    m_replayBarrier = SystemConf::getInstance().BUNDLE_SIZE / SystemConf::getInstance().BATCH_SIZE;
    if (m_replayBarrier == 0)
      throw std::runtime_error(
          "error: the bundle size should be greater or equal to the batch size when replaying data with range-based windows");
  }
}

void TaskDispatcher::dispatch(char *data, int length, long latencyMark) {
  long idx;
  while ((idx = m_buffer.put(data, length, latencyMark)) < 0) {
    std::this_thread::yield();
  }
  assemble(idx, length);
}

bool TaskDispatcher::tryDispatch(char *data, int length, long latencyMark) {
  long idx;
  if ((idx = m_buffer.put(data, length, latencyMark)) < 0) {
    return false;
  }
  assemble(idx, length);
  return true;
}

QueryBuffer *TaskDispatcher::getBuffer() {
  return &m_buffer;
}

void TaskDispatcher::setTaskQueue(std::shared_ptr<TaskQueue> queue) {
  m_workerQueue = queue;
}

long TaskDispatcher::getBytesGenerated() {
  return m_parent.getBytesGenerated();
}

TaskDispatcher::~TaskDispatcher() {}

void TaskDispatcher::assemble(long index, int length) {
  if (SystemConf::getInstance().LATENCY_ON) {
    if (m_latencyMark < 0) {
      //latencyMark = index;
      // get latency mark
      auto systemTimestamp = getSystemTimestamp(index);
      m_latencyMark = systemTimestamp;
      // reset the correct timestamp for the first tuple
      auto tupleTimestamp = getTimestamp(index);
      setTimestamp((int) index, tupleTimestamp);
    }
  }

  m_accumulated += (length);
  if ((!m_window.isRangeBased()) && (!m_window.isRowBased())) {
    throw std::runtime_error("error: window is neither row-based nor range-based");
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
        m_thisBatchStartPointer % m_mask,
        m_nextBatchEndPointer % m_mask,
#else
    m_thisBatchStartPointer & m_mask,
    m_nextBatchEndPointer   & m_mask,
#endif
        m_f,
        m_thisBatchStartPointer,
        m_nextBatchEndPointer
    );

    m_thisBatchStartPointer += m_batchSize;
    m_nextBatchEndPointer += m_batchSize;
  }
}

void TaskDispatcher::newTaskFor(long p, long q, long free, long b_, long _d) {

  int taskId = getTaskNumber();
  long size = 0;
  size = (q <= p) ? (q + m_buffer.getCapacity()) - p : q - p;
  /*std::cout << "[DBG] Query "+std::to_string(m_parent.getId())+" task "+std::to_string(taskId)+
      " ["+std::to_string(p)+", "+std::to_string(q)+"), free "+std::to_string(free)+", ["+std::to_string(b_)+
      ", "+std::to_string(_d)+"] size "+std::to_string(size) << std::endl;*/

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
    std::cout
        << "error: negative free pointer (" + std::to_string(free) + ") for query " + std::to_string(m_parent.getId());
    exit(1);
  }

  auto batch = WindowBatchFactory::getInstance().
      newInstance(m_batchSize, taskId, (int) (free), &m_parent,
                  &m_buffer, &m_window, m_schema, mark);

  if (m_window.isRangeBased()) {
    long startTime = getTimestamp((int) (p));
    long endTime = getTimestamp((int) (q - m_tupleSize));
    batch->setBatchTimestamps(startTime, endTime);
    if (m_replayTimestamps) {
      long prevStartTime = 0;
      long prevEndTime = 0;
      if (m_step != -1) {
        prevStartTime = (p != 0) ?
                        getTimestamp((int) (p - m_batchSize - m_tupleSize)) :
                        getTimestamp((int) (m_buffer.getCapacity() - m_batchSize - m_tupleSize));
        prevEndTime = (p != 0) ?
                      getTimestamp((int) (p - m_tupleSize)) :
                      getTimestamp((int) (m_buffer.getCapacity() - m_tupleSize));
        if (startTime + m_offset - prevEndTime >= m_step) // sanity check
          m_offset -= m_step;
      }
      setTimestamp((int) (p), startTime + m_offset);
      setTimestamp((int) (q - m_tupleSize), endTime + m_offset);
      batch->setBatchTimestamps(startTime + m_offset, endTime + m_offset);
      batch->setPrevTimestamps(prevStartTime, prevEndTime);
      /*std::cout << "[DBG] " << taskId << " taskId " << m_step << " step "<< m_offset << " offset "
                << p << " startIdx " << q << " endIdx " << prevEndTime << " prevEndTime "
                << startTime << " initialStartTime " << endTime << " initialEndTime "
                << getTimestamp((int) (p)) << " startTime " << getTimestamp((int) (q - m_tupleSize)) << " endTime " << std::endl;*/
      batch->setTimestampOffset(m_offset);
      if (taskId % (m_replayBarrier) == 0) {
        if (m_step == -1)
          m_step = endTime + 1;
        if (_d <= (long) m_buffer.getCapacity()) //SystemConf::getInstance().CIRCULAR_BUFFER_SIZE)
          m_offset += m_step;
        //batch->setBatchTimestamps(startTime, endTime+offset);
      }
    }
  } else {
    batch->setBatchTimestamps(-1, -1);
  }

  batch->setBufferPointers((int) p, (int) q);
  batch->setStreamPointers(b_, _d);

  auto task = TaskFactory::getInstance().newInstance(taskId, batch);

  while (!m_workerQueue->try_enqueue(task));
}

int TaskDispatcher::getTaskNumber() {
  int id = m_nextTask++;
  if (m_nextTask == INT_MAX)
    m_nextTask = 1;
  return id;
}

long TaskDispatcher::getTimestamp(int index) {
  // wrap around if it gets out of bounds
  if (index < 0)
    index = m_buffer.getCapacity() + index;
  long value = m_buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long) Utils::getTupleTimestamp(value);
  else
    return value;
}

long TaskDispatcher::getSystemTimestamp(int index) {
  // wrap around if it gets out of bounds
  if (index < 0)
    index = m_buffer.getCapacity() + index;
  long value = m_buffer.getLong(index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long) Utils::getSystemTimestamp(value);
  else
    return value;
}

void TaskDispatcher::setTimestamp(int index, long timestamp) {
  // wrap around if it gets out of bounds
  if (index < 0)
    index = m_buffer.getCapacity() + index;
  m_buffer.setLong(index, timestamp);
}