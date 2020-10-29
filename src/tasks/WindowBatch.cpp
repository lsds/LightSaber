#include "tasks/WindowBatch.h"
#include "utils/Utils.h"
#include "buffers/NUMACircularQueryBuffer.h"
#include "buffers/PartialWindowResults.h"
#include "utils/Query.h"
#include "utils/TupleSchema.h"
#include "utils/WindowDefinition.h"

#include <algorithm>
#include <buffers/PartialWindowResultsFactory.h>

/*
 *
 *
 * */

WindowBatch::WindowBatch(size_t batchSize, int taskId, int freePointer,
                         Query *query, QueryBuffer *buffer,
                         WindowDefinition *windowDefinition, TupleSchema *schema, long mark) :
    m_batchSize(batchSize), m_taskId(taskId), m_freePointer(freePointer), m_query(query), m_inputBuffer(buffer),
    m_openingWindows(nullptr), m_closingWindows(nullptr), m_pendingWindows(nullptr), m_completeWindows(nullptr),
    m_windowDefinition(windowDefinition), m_schema(schema), m_latencyMark(mark), m_startPointer(-1), m_endPointer(-1),
    m_streamStartPointer(-1), m_streamEndPointer(-1), m_startTimestamp(-1), m_endTimestamp(-1),
    m_windowStartPointers(SystemConf::getInstance().PARTIAL_WINDOWS),
    m_windowEndPointers(SystemConf::getInstance().PARTIAL_WINDOWS), m_lastWindowIndex(0),
    m_fragmentedWindows(false), m_hasPendingWindows(false), m_initialised(false) {}

void WindowBatch::set(size_t batchSize, int taskId, int freePointer,
                      Query *query, QueryBuffer *buffer,
                      WindowDefinition *windowDefinition, TupleSchema *schema, long mark) {

  m_batchSize = batchSize;
  m_taskId = taskId;
  m_freePointer = freePointer;
  m_query = query;
  m_inputBuffer = buffer;
  m_windowDefinition = windowDefinition;
  m_schema = schema;
  m_latencyMark = mark;

  m_startPointer = -1;
  m_endPointer = -1;
  m_streamStartPointer = -1;
  m_streamEndPointer = -1;
  m_startTimestamp = -1;
  m_endTimestamp = -1;

  m_lastWindowIndex = 0;
  m_fragmentedWindows = false;
  m_hasPendingWindows = false;
  m_openingWindows = m_closingWindows = m_pendingWindows = m_completeWindows = nullptr;
  m_initialised = false;
  m_replayTimestamps = false;
  m_offset = 0;

#if defined(HAVE_NUMA)
  m_numaNodeId = ((NUMACircularQueryBuffer *) m_inputBuffer)->geNumaNodeWithPtr(m_freePointer);
#else
  m_numaNodeId = 0;
#endif
}

int WindowBatch::getBatchSize() {
  return (int) m_batchSize;
}

int WindowBatch::getTaskId() {
  return m_taskId;
}

void WindowBatch::setTaskId(int taskId) {
  m_taskId = taskId;
}

int WindowBatch::getNumaNodeId() {
  return m_numaNodeId;
}

Query *WindowBatch::getQuery() {
  return m_query;
}

void WindowBatch::setQuery(Query *query) {
  m_query = query;
}

int WindowBatch::getPid() {
  return m_pid;
}

void WindowBatch::setPid(int pid) {
  m_pid = pid;
}

TaskType WindowBatch::getTaskType() {
  return m_type;
}

void WindowBatch::setTaskType(TaskType taskType) {
  m_type = taskType;
}

QueryBuffer *WindowBatch::getInputQueryBuffer() {
  return m_inputBuffer;
}

ByteBuffer &WindowBatch::getBuffer() {
#if defined(HAVE_NUMA)
  return ((NUMACircularQueryBuffer *)m_inputBuffer)->getBuffer(m_numaNodeId);
#else
  return m_inputBuffer->getBuffer();
#endif
}

char *WindowBatch::getBufferRaw() {
#if defined(HAVE_NUMA)
  NUMACircularQueryBuffer *b = dynamic_cast<NUMACircularQueryBuffer *>(m_inputBuffer);
  assert(b != nullptr && "error: invalid buffer pointer");
  return b->getBufferRaw(m_numaNodeId);
#else
  return m_inputBuffer->getBufferRaw();
#endif
}

void WindowBatch::setOutputBuffer(std::shared_ptr<PartialWindowResults> buffer) {
  m_outputBuffer = buffer;
}

std::shared_ptr<PartialWindowResults> WindowBatch::getOutputBuffer() {
  return m_outputBuffer;
}

TupleSchema *WindowBatch::getSchema() {
  return m_schema;
}

void WindowBatch::setSchema(TupleSchema *schema) {
  m_schema = schema;
}

WindowDefinition *WindowBatch::getWindowDefinition() {
  return m_windowDefinition;
}

int WindowBatch::getFreePointer() {
  return m_freePointer;
}

int WindowBatch::getBufferStartPointer() {
  return m_startPointer;

}

int WindowBatch::getBufferEndPointer() {
  return m_endPointer;
}

void WindowBatch::setBufferPointers(int startP, int endP) {
#if defined(HAVE_NUMA)
  m_startPointer = startP % ((NUMACircularQueryBuffer*)m_inputBuffer)->getBuffer(m_numaNodeId).size();
  m_endPointer = endP % ((NUMACircularQueryBuffer*)m_inputBuffer)->getBuffer(m_numaNodeId).size();
  if (m_endPointer == 0)
      m_endPointer = ((NUMACircularQueryBuffer*)m_inputBuffer)->getBuffer(m_numaNodeId).size();
#else
  m_startPointer = startP;
  m_endPointer = endP;
#endif
}

long WindowBatch::getStreamStartPointer() {
  return m_streamStartPointer;
}

long WindowBatch::getStreamEndPointer() {
  return m_streamEndPointer;
}

void WindowBatch::setStreamPointers(long startP, long endP) {
  m_streamStartPointer = startP;
  m_streamEndPointer = endP;
}

long WindowBatch::getBatchStartTimestamp() {
  return m_startTimestamp;
}

long WindowBatch::getBatchEndTimestamp() {
  return m_endTimestamp;
}

void WindowBatch::setBatchTimestamps(long startP, long endP) {
  m_startTimestamp = startP;
  m_endTimestamp = endP;
}

long WindowBatch::getLatencyMark() {
  return m_latencyMark;
}

void WindowBatch::setLatencyMark(long mark) {
  m_latencyMark = mark;
}

void WindowBatch::setTimestampOffset(long value) {
  m_offset = value;
  m_replayTimestamps = true;
}

bool WindowBatch::hasTimestampOffset() {
  return m_replayTimestamps;
}
void WindowBatch::updateTimestamps() {
#if defined(HAVE_NUMA)
  auto buf = (long *) ((NUMACircularQueryBuffer*)m_inputBuffer)->getBuffer(m_numaNodeId).data();
#else
  auto buf = (long *) m_inputBuffer->getBuffer().data();
#endif

  auto tupleSize = m_schema->getTupleSize();
  auto startPos = m_startPointer / sizeof(long);
  auto endPos = m_endPointer / sizeof(long);
  auto step = tupleSize / sizeof(long);
  for (unsigned long i = startPos + step; i < endPos - step; i += step) // the first and last timestamp are already set
    buf[i] += m_offset;
}

std::vector<long> &WindowBatch::getWindowStartPointers() {
  return m_windowStartPointers;
}

std::vector<long> &WindowBatch::getWindowEndPointers() {
  return m_windowEndPointers;
}

bool WindowBatch::containsFragmentedWindows() {
  return m_fragmentedWindows;
}

bool WindowBatch::containsPendingWindows() {
  return m_hasPendingWindows;
}

std::shared_ptr<PartialWindowResults> WindowBatch::getOpeningWindows() {
  return m_openingWindows;
}

void WindowBatch::setOpeningWindows(std::shared_ptr<PartialWindowResults> results) {
  m_fragmentedWindows = true;
  m_openingWindows = results;
}

std::shared_ptr<PartialWindowResults> WindowBatch::getClosingWindows() {
  return m_closingWindows;
}

void WindowBatch::setClosingWindows(std::shared_ptr<PartialWindowResults> results) {
  m_fragmentedWindows = true;
  m_closingWindows = results;
}

std::shared_ptr<PartialWindowResults> WindowBatch::getPendingWindows() {
  return m_pendingWindows;
}

void WindowBatch::setPendingWindows(std::shared_ptr<PartialWindowResults> results) {
  m_fragmentedWindows = true;
  m_pendingWindows = results;
}

std::shared_ptr<PartialWindowResults> WindowBatch::getCompleteWindows() {
  return m_completeWindows;
}

void WindowBatch::setCompleteWindows(std::shared_ptr<PartialWindowResults> results) {
  m_fragmentedWindows = true;
  m_completeWindows = results;
}

int WindowBatch::getLastWindowIndex() {
  return m_lastWindowIndex;
}

void WindowBatch::clear() {
  m_initialised = false;
  m_openingWindows.reset();
  m_closingWindows.reset();
  m_pendingWindows.reset();
  m_completeWindows.reset();
  m_outputBuffer.reset();
}

void WindowBatch::resetWindowPointers() {
  std::fill(m_windowStartPointers.begin(), m_windowStartPointers.end(), -1);
  std::fill(m_windowEndPointers.begin(), m_windowEndPointers.end(), -1);
}

int WindowBatch::normalise(int pointer) {
  return (int) m_inputBuffer->normalise((long) pointer);
}

long WindowBatch::getTimestamp(int index) {
  long value = m_inputBuffer->getLong((size_t) index);
  if (SystemConf::getInstance().LATENCY_ON)
    return (long) Utils::getTupleTimestamp(value);
  else
    return value;
}

void WindowBatch::setPrevTimestamps(long startTime, long endTime) {
  m_prevStartTimestamp = startTime;
  m_prevEndTimestamp = endTime;
}

long WindowBatch::getPrevStartTimestamp() {
  return m_prevStartTimestamp;
}

long WindowBatch::getPrevEndTimestamp() {
  return m_prevEndTimestamp;
}

void WindowBatch::setEmptyWindowIds(long emptyStartWindow, long emptyEndWindow) {
  m_emptyStartWindowId = emptyStartWindow;
  m_emptyEndWindowId = emptyEndWindow;
}

long WindowBatch::getEmptyStartWindowId() {
  return m_emptyStartWindowId;
}

long WindowBatch::getEmptyEndWindowId() {
  return m_emptyEndWindowId;
}

void WindowBatch::initPartialWindowPointers() {
  if (m_initialised)
    throw std::runtime_error("error: batch window pointers already initialised");
  if (m_windowDefinition->isRangeBased())
    initPartialRangeBasedWindowPointers();
  else
    initPartialCountBasedWindowPointers();
  m_initialised = true;
}

void WindowBatch::initPartialRangeBasedWindowPointers() {
  int tupleSize = m_schema->getTupleSize();
  long paneSize = m_windowDefinition->getPaneSize();
  std::fill(m_windowStartPointers.begin(), m_windowStartPointers.end(), -1);
  std::fill(m_windowEndPointers.begin(), m_windowEndPointers.end(), -1);

  /* Slicing based on panes logic */
  long streamPtr;
  int bufferPtr;
  /* Previous, next, and current pane ids */
  long _pid, pid_, pid = 0;
  long pane;
  /* Current window */
  long wid;
  long offset = -1;
  int numberOfOpeningWindows = 0; /* Counters */
  int numberOfClosingWindows = 0;
  /* Set previous pane id */
  if (m_streamStartPointer == 0) {
    _pid = -1;
  } else {
    /* Check the last tuple of the previous batch */
    _pid = (getTimestamp(m_startPointer - m_schema->getTupleSize()) / paneSize);
  }
  /* Set offset */
  if (m_streamStartPointer == 0)
    offset = 0;
  for (streamPtr = m_streamStartPointer, bufferPtr = m_startPointer; streamPtr < m_streamEndPointer && bufferPtr < m_endPointer;
       streamPtr += tupleSize, bufferPtr += tupleSize) {
    pid = getTimestamp(bufferPtr) / paneSize; /* Current pane */
    if (_pid < pid) {
      /* Pane `_pid` closed; pane `pid` opened; iterate over panes in between */
      while (_pid < pid) {
        pid_ = _pid + 1;
        /* Check if a window closes at this pane */
        pane = pid_ - m_windowDefinition->numberOfPanes();
        if (pane >= 0 && pane % m_windowDefinition->panesPerSlide() == 0) {
          wid = pane / m_windowDefinition->panesPerSlide();
          if (wid >= 0) {
            /* Calculate offset */
            if (offset < 0) {
              offset = wid;
            } else {
              /* The offset has already been set */
              if (numberOfClosingWindows == 0 && m_streamStartPointer != 0) {
                /* Shift down */
                int delta = (int) (offset - wid);
                for (int i = m_lastWindowIndex; i >= 0; i--) {
                  m_windowStartPointers[i + delta] = m_windowStartPointers[i];
                  m_windowEndPointers[i + delta] = m_windowEndPointers[i];
                }
                for (int i = 0; i < delta; i++) {
                  m_windowStartPointers[i] = -1;
                  m_windowEndPointers[i] = -1;
                }
                /* Set last window index */
                m_lastWindowIndex += delta;
                /* Reset offset */
                offset = wid;
              }
            }
            int index = (int) (wid - offset);
            if (index < 0) {
              std::cout << "error: failed to close window " + std::to_string(wid) << std::endl;
              exit(1);
            }
            /* Store end pointer */
            m_windowEndPointers[index] = bufferPtr;
            numberOfClosingWindows += 1;
            /*
             * Has this window been previously opened?
             *
             * We characterise this window as "closing" and we expect to find its
             * match in the opening set of the previous batch. But if this is the
             * first batch, then there will be none.
             */
            if (m_windowStartPointers[index] < 0 && m_streamStartPointer == 0)
              m_windowStartPointers[index] = 0;
            m_lastWindowIndex = (m_lastWindowIndex < index) ? index : m_lastWindowIndex;
          }
        }
        /* Check if a window opens at `pid_` */
        if (pid_ % m_windowDefinition->panesPerSlide() == 0) {
          wid = pid_ / m_windowDefinition->panesPerSlide();
          /* Calculate offset */
          if (offset < 0) {
            offset = wid;
          }
          /* Store start pointer */
          int index = (int) (wid - offset);
          m_windowStartPointers[index] = bufferPtr;
          numberOfOpeningWindows += 1;
          m_lastWindowIndex = (m_lastWindowIndex < index) ? index : m_lastWindowIndex;
        }
        _pid += 1;
      } /* End while */
      _pid = pid;
    } /* End if */
  } /* End for */
  if (numberOfOpeningWindows > 0 && numberOfClosingWindows == 0 && m_streamStartPointer != 0) {
    /* There are no closing windows. Therefore, windows that
     * have opened in a previous batch, should be considered
     * as pending. */
    for (int i = m_lastWindowIndex; i >= 0; i--) {
      m_windowStartPointers[i + 1] = m_windowStartPointers[i];
      m_windowEndPointers[i + 1] = m_windowEndPointers[i];
    }
    /* Set pending window */
    m_windowStartPointers[0] = -1;
    m_windowEndPointers[0] = -1;
    /* Increment last window index */
    m_lastWindowIndex++;
  } else if (numberOfOpeningWindows == 0 && numberOfClosingWindows == 0) {
    /* There are only pending windows in the batch */
    m_lastWindowIndex = 0;
  }
}

void WindowBatch::initPartialCountBasedWindowPointers() {
  int tupleSize = m_schema->getTupleSize();
  long paneSize = m_windowDefinition->getPaneSize();
  std::fill(m_windowStartPointers.begin(), m_windowStartPointers.end(), -1);
  std::fill(m_windowEndPointers.begin(), m_windowEndPointers.end(), -1);

  /* Slicing based on panes logic */
  long streamPtr;
  int bufferPtr;
  /* Previous, next, and current pane ids */
  long _pid, pid_, pid = 0;
  long pane; /* Normalised to panes/window */
  /* Current window */
  long wid;
  long offset = -1;
  int numberOfOpeningWindows = 0; /* Counters */
  int numberOfClosingWindows = 0;
  /* Set previous pane id */
  if (m_streamStartPointer == 0) {
    _pid = -1;
  } else {
    _pid = ((m_streamStartPointer / tupleSize) / paneSize) - 1;
  }
  /* Set offset */
  if (m_streamStartPointer == 0)
    offset = 0;

  for (streamPtr = m_streamStartPointer, bufferPtr = m_startPointer; streamPtr < m_streamEndPointer && bufferPtr < m_endPointer;
       streamPtr += tupleSize, bufferPtr += tupleSize) {
    /* Current pane */
    pid = (streamPtr / tupleSize) / paneSize;
    if (_pid < pid) {
      /* Pane `_pid` closed; pane `pid` opened; iterate over panes in between... */
      while (_pid < pid) {
        pid_ = _pid + 1;
        /* Check if a window closes at this pane */
        pane = pid_ - m_windowDefinition->numberOfPanes();
        if (pane >= 0 && (pane % m_windowDefinition->panesPerSlide() == 0)) {
          wid = pane / m_windowDefinition->panesPerSlide();
          if (wid >= 0) {
            /* Calculate offset */
            if (offset < 0) {
              offset = wid;
            } else {
              /* The offset has already been set */
              if (numberOfClosingWindows == 0 && m_streamStartPointer != 0) {
                /* Shift down */
                int delta = (int) (offset - wid);
                for (int i = m_lastWindowIndex; i >= 0; i--) {
                  m_windowStartPointers[i + delta] = m_windowStartPointers[i];
                  m_windowEndPointers[i + delta] = m_windowEndPointers[i];
                }
                for (int i = 0; i < delta; i++) {
                  m_windowStartPointers[i] = -1;
                  m_windowEndPointers[i] = -1;
                }
                /* Set last window index */
                m_lastWindowIndex += delta;
                /* Reset offset */
                offset = wid;
              }
            }
            int index = (int) (wid - offset);
            if (index < 0) {
              std::cout << "error: failed to close window " + std::to_string(wid) << std::endl;
              exit(1);
            }
            /* Store end pointer */
            m_windowEndPointers[index] = bufferPtr;
            numberOfClosingWindows += 1;
            /*
             * Has this window been previously opened?
             *
             * We characterise this window as "closing" and we expect to find its
             * match in the opening set of the previous batch. But if this is the
             * first batch, then there will be none.
             */
            if (m_windowStartPointers[index] < 0 && m_streamStartPointer == 0)
              m_windowStartPointers[index] = 0;
            m_lastWindowIndex = (m_lastWindowIndex < index) ? index : m_lastWindowIndex;
          }
        }
        /* Check if a window opens at `pid_` */
        if (pid_ % m_windowDefinition->panesPerSlide() == 0) {
          wid = pid_ / m_windowDefinition->panesPerSlide();
          /* Calculate offset */
          if (offset < 0) {
            offset = wid;
          }
          /* Store start pointer */
          int index = (int) (wid - offset);
          m_windowStartPointers[index] = bufferPtr;
          numberOfOpeningWindows += 1;
          m_lastWindowIndex = (m_lastWindowIndex < index) ? index : m_lastWindowIndex;
        }
        _pid += 1;
      } /* End while */
      _pid = pid;
    } /* End if */
  } /* End for */
  if (numberOfOpeningWindows > 0 && numberOfClosingWindows == 0 && m_streamStartPointer != 0) {
    /* There are no closing windows. Therefore, windows that
     * have opened in a previous batch, should be considered
     * as pending. */
    for (int i = m_lastWindowIndex; i >= 0; i--) {
      m_windowStartPointers[i + 1] = m_windowStartPointers[i];
      m_windowEndPointers[i + 1] = m_windowEndPointers[i];
    }
    /* Set pending window */
    m_windowStartPointers[0] = -1;
    m_windowEndPointers[0] = -1;
    /* Increment last window index */
    m_lastWindowIndex++;
  } else if (numberOfOpeningWindows == 0 && numberOfClosingWindows == 0) {
    /* There are only pending windows in the batch */
    m_lastWindowIndex = 0;
  }
}