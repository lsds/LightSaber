#pragma once

#include <vector>
#include <memory>
#include <climits>

#include "buffers/QueryBuffer.h"

class PartialWindowResults;
class WindowDefinition;
class TupleSchema;
class Query;

/*
 * \brief This class holds the input/output buffers with the respective start/end pointer
 * required for processing.
 *
 * There is no support for multiple concurrent queries yet.
 *
 * */

class WindowBatch {
 private:
  size_t m_batchSize;
  int m_taskId;
  int m_numaNodeId;
  int m_freePointer;
  Query *m_query;
  QueryBuffer *m_inputBuffer;
  /* buffer holding the results when no window semantics are required */
  std::shared_ptr<PartialWindowResults> m_outputBuffer;
  /* buffers holding the results of window fragments */
  std::shared_ptr<PartialWindowResults> m_openingWindows, m_closingWindows, m_pendingWindows, m_completeWindows;
  WindowDefinition *m_windowDefinition;
  TupleSchema *m_schema;

  long m_latencyMark;
  long m_startPointer;
  long m_endPointer;
  long m_streamStartPointer;
  long m_streamEndPointer;
  long m_startTimestamp;
  long m_endTimestamp;
  long m_prevStartTimestamp;
  long m_prevEndTimestamp;
  long m_emptyStartWindowId;
  long m_emptyEndWindowId;

  std::vector<long> m_windowStartPointers;
  std::vector<long> m_windowEndPointers;
  int m_lastWindowIndex;
  bool m_fragmentedWindows;
  bool m_hasPendingWindows;
  bool m_initialised;
  bool m_replayTimestamps = false;
  long m_offset = 0;

 public:
  WindowBatch(size_t batchSize = 0, int taskId = 0, int freePointer = INT_MIN,
              Query *query = nullptr, QueryBuffer *buffer = nullptr,
              WindowDefinition *windowDefinition = nullptr, TupleSchema *schema = nullptr, long mark = 0);
  void set(size_t batchSize, int taskId, int freePointer,
           Query *query, QueryBuffer *buffer,
           WindowDefinition *windowDefinition, TupleSchema *schema, long mark);
  int getBatchSize();
  int getTaskId();
  void setTaskId(int taskId);
  int getNumaNodeId();
  Query *getQuery();
  void setQuery(Query *query);
  QueryBuffer *getInputQueryBuffer();
  ByteBuffer &getBuffer();
  void setOutputBuffer(std::shared_ptr<PartialWindowResults> buffer);
  std::shared_ptr<PartialWindowResults> getOutputBuffer();
  std::shared_ptr<PartialWindowResults> getOpeningWindows();
  void setOpeningWindows(std::shared_ptr<PartialWindowResults> results);
  std::shared_ptr<PartialWindowResults> getClosingWindows();
  void setClosingWindows(std::shared_ptr<PartialWindowResults> results);
  std::shared_ptr<PartialWindowResults> getPendingWindows();
  void setPendingWindows(std::shared_ptr<PartialWindowResults> results);
  std::shared_ptr<PartialWindowResults> getCompleteWindows();
  void setCompleteWindows(std::shared_ptr<PartialWindowResults> results);
  TupleSchema *getSchema();
  void setSchema(TupleSchema *schema);
  WindowDefinition *getWindowDefinition();
  int getFreePointer();
  int getBufferStartPointer();
  int getBufferEndPointer();
  void setBufferPointers(int startP, int endP);
  long getStreamStartPointer();
  long getStreamEndPointer();
  void setStreamPointers(long startP, long endP);
  long getBatchStartTimestamp();
  long getBatchEndTimestamp();
  void setBatchTimestamps(long startP, long endP);
  long getLatencyMark();
  void setLatencyMark(long mark);
  void setTimestampOffset(long offset);
  bool hasTimestampOffset();
  void updateTimestamps();
  std::vector<long> &getWindowStartPointers();
  std::vector<long> &getWindowEndPointers();
  bool containsFragmentedWindows();
  bool containsPendingWindows();
  int getLastWindowIndex();
  void clear();
  void resetWindowPointers();
  int normalise(int pointer);
  long getTimestamp(int index);
  void setPrevTimestamps(long startTime, long endTime);
  long getPrevStartTimestamp();
  long getPrevEndTimestamp();
  void setEmptyWindowIds(long emptyStartWindow, long emptyEndWindow);
  long getEmptyStartWindowId();
  long getEmptyEndWindowId();
  void initPartialWindowPointers();
  void initPartialRangeBasedWindowPointers();
  void initPartialCountBasedWindowPointers();
};