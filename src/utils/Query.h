#pragma once

#include "utils/SystemConf.h"
#include "WindowDefinition.h"

#include <vector>
#include <string>

class ITaskDispatcher;
class WindowDefinition;
class QueryApplication;
class TupleSchema;
class QueryOperator;
class ResultHandler;
class AggregateOperatorCode;
class QueryBuffer;
class LatencyMonitor;
class QueryConfig;

/*
 * \brief This class represents a sequence of pipelineable operators
 * that get translated to a single worker task.
 *
 * At the moment no stream-to-stream join is supported.
 * By setting appropriately the @replayTimestamps and @copyDataOnInsert variables,
 * data are replayed from in-memory with new timestamps in the case of time-based windows.
 *
 * */

class Query {
 private:
  WindowDefinition m_firstWindow, m_secondWindow;
  TupleSchema *m_firstSchema, *m_secondSchema;
  TupleSchema *m_outputSchema;
  QueryConfig *m_config = nullptr;
  std::shared_ptr<QueryBuffer> m_firstCircularBuffer, m_secondCircularBuffer;
  size_t m_taskQueueCapacity;
  std::shared_ptr<TaskQueue> m_taskQueue;
  std::shared_ptr<ITaskDispatcher> m_dispatcher;
  std::shared_ptr<ResultHandler> m_resultHandler;
  long m_timestampReference = 0L;
  std::unique_ptr<LatencyMonitor> m_latencyMonitor;
  std::vector<QueryOperator *> m_operators;
  QueryOperator *m_mostUpstreamOperator;
  QueryOperator *m_mostDownstreamOperator;

  int m_id;
  int m_numberOfUpstreamQueries;
  int m_numberOfDownstreamQueries;
  std::string m_name;
  std::string m_sql;

  QueryApplication *m_parent = nullptr;
  std::vector<Query *> m_upstreamQueries;
  std::vector<Query *> m_downstreamQueries;

  bool m_isLeft = false;
  int m_numOfWindowDefinitions = 0;
  bool m_dropTasks = false;
  int m_taskToDrop = -1;

  bool m_markedForCheckpoint;

 public:
  Query(int id, std::vector<QueryOperator *> &operators, WindowDefinition window,
        TupleSchema *schema = nullptr, long timestampReference = 0L, bool hasWindowFragments = false,
        bool replayTimestamps = false, bool copyDataOnInsert = true, bool useParallelMerge = false,
        int multipleQueries = 0, bool persistInput = false, QueryConfig *config = nullptr, bool clearFiles = true);
  Query(int id, std::vector<QueryOperator *> &operators, WindowDefinition firstWindow,
        TupleSchema *firstSchema, WindowDefinition secondWindow,
        TupleSchema *secondSchema, long timestampReference = 0L, bool hasWindowFragments = false,
        bool replayTimestamps = false, bool copyDataOnInsert = true, bool useParallelMerge = false,
        int multipleQueries = 0, bool persistInput = false, QueryConfig *config = nullptr, bool clearFiles = true);
  int getSchemaTupleSize();
  void setName(std::string name);
  void setSQLExpression(std::string sql);
  std::string getName();
  std::string getSQLExpression();
  int getId();
  bool isMostUpstream();
  bool isMostDownstream();
  QueryConfig *getConfig();
  QueryOperator *getMostUpstreamOperator();
  QueryOperator *getMostDownstreamOperator();
  QueryOperator *getOperator();
  QueryApplication *getParent();
  void setParent(QueryApplication *parent);
  QueryBuffer *getBuffer();
  QueryBuffer *getSecondBuffer();
  std::shared_ptr<ITaskDispatcher> getTaskDispatcher();
  void setTaskQueue(std::shared_ptr<TaskQueue> &queue);
  std::shared_ptr<TaskQueue> getTaskQueue();
  size_t getTaskQueueCapacity();
  std::shared_ptr<ResultHandler> getResultHandler();
  void setAggregateOperator(AggregateOperatorCode *aggrOperator);
  long getBytesGenerated();
  void startDroppingTasks(int task);
  bool isTaskDropped(int task);
  long getTimestampReference();
  long getLastTimestamp();
  WindowDefinition &getWindowDefinition();
  TupleSchema *getSchema();
  WindowDefinition &getFirstWindowDefinition();
  TupleSchema *getFirstSchema();
  WindowDefinition &getSecondWindowDefinition();
  TupleSchema *getSecondSchema();
  TupleSchema *getOutputSchema();
  int getNumOfWindowDefinitions();
  void setOutputSchema(TupleSchema *schema);
  LatencyMonitor &getLatencyMonitor();
  void connectTo(Query *query);
  bool getIsLeft();
  void markForCheckpoint(bool mark);
  bool isMarkedForCheckpoint();
  Query *getUpstreamQuery();
  Query *getUpstreamQuery(int idx);
  Query *getDownstreamQuery();
  Query *getDownstreamQuery(int idx);
  int getNumberOfUpstreamQueries();
  int getNumberOfDownstreamQueries();
  ~Query();

 private:
  void setUpstreamQuery(Query *query);
  void setLeft(bool isLeft);
};