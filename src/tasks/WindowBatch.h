#pragma once

#include <vector>
#include <memory>
#include <climits>

#include "buffers/QueryBuffer.h"

class PartialWindowResults;
class WindowDefinition;
class TupleSchema;
class Query;
struct LineageGraph;
enum TaskType : uint8_t;

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
  int m_pid;
  int m_numaNodeId;
  long m_freePointer1, m_freePointer2;
  long m_prevFreePointer1, m_prevFreePointer2;
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

  TaskType m_type;

  std::shared_ptr<LineageGraph> m_graph;

  // variables added for watermarks
  long m_watermark = LONG_MIN;
  char * m_partialBuffer;

 public:
  WindowBatch(size_t batchSize = 0, int taskId = 0,
              long freePointer1 = INT_MIN, long freePointer2 = INT_MIN,
              Query *query = nullptr, QueryBuffer *buffer = nullptr,
              WindowDefinition *windowDefinition = nullptr, TupleSchema *schema = nullptr, long mark = 0,
              long prevFreePointer1 = -1, long prevFreePointer2 = -1);
  void set(size_t batchSize, int taskId, long freePointer1,
           long freePointer2, Query *query, QueryBuffer *buffer,
           WindowDefinition *windowDefinition, TupleSchema *schema, long mark,
           long prevFreePointer1 = -1, long prevFreePointer2 = -1);
  int getBatchSize();
  int getTaskId();
  void setTaskId(int taskId);
  int getNumaNodeId();
  Query *getQuery();
  void setQuery(Query *query);
  int getPid();
  void setPid(int pid);
  TaskType getTaskType();
  void setTaskType(TaskType taskType);
  QueryBuffer *getInputQueryBuffer();
  ByteBuffer &getBuffer();
  char *getBufferRaw();
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
  void setLineageGraph(std::shared_ptr<LineageGraph> &graph);
  std::shared_ptr<LineageGraph> &getLineageGraph();
  long getFreePointer();
  long getSecondFreePointer();
  long getPrevFreePointer();
  long getPrevSecondFreePointer();
  long getBufferStartPointer();
  long getBufferEndPointer();
  void setBufferPointers(long startP, long endP);
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
  long normalise(long pointer);
  long getTimestamp(long index);
  void setPrevTimestamps(long startTime, long endTime);
  long getPrevStartTimestamp();
  long getPrevEndTimestamp();
  void setEmptyWindowIds(long emptyStartWindow, long emptyEndWindow);
  long getEmptyStartWindowId();
  long getEmptyEndWindowId();

  // functions for watermarks
  void setWatermark (long watermark);
  long getWatermark ();
  void setPartialBuffer (char *partial);
  char *getPartialBuffer ();

  void initPartialWindowPointers();
  void initPartialRangeBasedWindowPointers();
  void initPartialCountBasedWindowPointers();
};