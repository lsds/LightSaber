#include "utils/Query.h"

#include <memory>

#include "cql/operators/AggregateOperatorCode.h"
#include "cql/operators/Selection.h"
#include "dispatcher/JoinTaskDispatcher.h"
#include "dispatcher/TaskDispatcher.h"
#include "monitors/LatencyMonitor.h"
#include "result/ResultHandler.h"
#include "utils/QueryApplication.h"
#include "utils/QueryConfig.h"
#include "utils/QueryOperator.h"
#include "utils/TupleSchema.h"
#if defined(HAVE_NUMA)
#include "buffers/NumaCircularQueryBuffer.h"
#include "buffers/PersistentNumaCircularQueryBuffer.h"
#else
#include "buffers/CircularQueryBuffer.h"
#include "buffers/PersistentCircularQueryBuffer.h"
#endif

Query::Query(int id, std::vector<QueryOperator *> &operators, WindowDefinition window,
             TupleSchema *schema, long timestampReference, bool hasWindowFragments, bool replayTimestamps,
             bool copyDataOnInsert, bool useParallelMerge, int multipleQueries, bool persistInput, QueryConfig *config, bool clearFiles)
    : Query(id, operators, window, schema, WindowDefinition(), nullptr,
            timestampReference, hasWindowFragments, replayTimestamps,
            copyDataOnInsert, useParallelMerge, multipleQueries, persistInput, config, clearFiles) {}

Query::Query(int id, std::vector<QueryOperator *> &operators, WindowDefinition firstWindow,
      TupleSchema *firstSchema, WindowDefinition secondWindow,
      TupleSchema *secondSchema, long timestampReference, bool hasWindowFragments, bool replayTimestamps,
             bool copyDataOnInsert, bool useParallelMerge, int multipleQueries, bool persistInput, QueryConfig *config, bool clearFiles)
    : m_firstWindow(firstWindow), m_secondWindow(secondWindow), m_firstSchema(firstSchema), m_secondSchema(secondSchema), m_config(config),
#if defined(HAVE_NUMA)
      m_firstCircularBuffer(!persistInput ? std::shared_ptr<QueryBuffer>(std::make_shared<NumaCircularQueryBuffer>(id * 10,
                                                                         m_config ? m_config->getCircularBufferSize() :
                                                                         SystemConf::getInstance().CIRCULAR_BUFFER_SIZE, m_firstSchema->getTupleSize(), copyDataOnInsert,
                                                                         m_config ? m_config->getBatchSize() : SystemConf::getInstance().BATCH_SIZE, clearFiles)) :
                            std::make_shared<PersistentNumaCircularQueryBuffer>(id * 10,
                                                                                m_config ? m_config->getCircularBufferSize() :
                                                                                SystemConf::getInstance().CIRCULAR_BUFFER_SIZE, m_firstSchema->getTupleSize(), copyDataOnInsert,
                                                                                              m_config ? m_config->getBatchSize() : SystemConf::getInstance().BATCH_SIZE,
                                                                                              nullptr, clearFiles)),
      m_secondCircularBuffer(!persistInput || !secondSchema ? std::shared_ptr<QueryBuffer>(std::make_shared<NumaCircularQueryBuffer>(id * 10 + 1,(secondSchema == nullptr) ?
                                                                                   1 : m_config ? m_config->getCircularBufferSize() :
                                                                                   SystemConf::getInstance().CIRCULAR_BUFFER_SIZE, 1, copyDataOnInsert,
                                                                                   m_config ? m_config->getBatchSize() : SystemConf::getInstance().BATCH_SIZE, clearFiles)) :
                             std::make_shared<PersistentNumaCircularQueryBuffer>(id * 10 + 1,
                                                                                m_config ? m_config->getCircularBufferSize() :
                                                                                SystemConf::getInstance().CIRCULAR_BUFFER_SIZE, m_firstSchema->getTupleSize(), copyDataOnInsert,
                                                                                                                m_config ? m_config->getBatchSize() : SystemConf::getInstance().BATCH_SIZE,
                                                                                                                nullptr, clearFiles)),
#else
    m_firstCircularBuffer(!persistInput ? std::shared_ptr<QueryBuffer>(std::make_shared<CircularQueryBuffer>(id * 10,
                                                             m_config ? m_config->getCircularBufferSize() :
                                                             SystemConf::getInstance().CIRCULAR_BUFFER_SIZE,
                                                             m_firstSchema->getTupleSize(),
                                                             copyDataOnInsert,
                                                             m_config ? m_config->getBatchSize() :
                                                                        SystemConf::getInstance().BATCH_SIZE, clearFiles)) :
                          std::make_shared<PersistentCircularQueryBuffer>(id * 10,
                                                                m_config ? m_config->getCircularBufferSize() :
                                                                SystemConf::getInstance().CIRCULAR_BUFFER_SIZE,
                                                                m_firstSchema->getTupleSize(),
                                                                copyDataOnInsert, m_config ? m_config->getBatchSize() :
                                                                                  SystemConf::getInstance().BATCH_SIZE,
                                                                                          nullptr, clearFiles)),
    m_secondCircularBuffer(!persistInput || !secondSchema ? std::shared_ptr<QueryBuffer>(std::make_shared<CircularQueryBuffer>(id * 10 + 1, (secondSchema == nullptr) ? 0 :
                                                           m_config ? m_config->getCircularBufferSize() :
                                                           SystemConf::getInstance().CIRCULAR_BUFFER_SIZE,
                                                                 (secondSchema == nullptr) ? 1 : m_secondSchema->getTupleSize(),
                                                             copyDataOnInsert,
                                                             m_config ? m_config->getBatchSize() :
                                                                        SystemConf::getInstance().BATCH_SIZE, clearFiles)) :
                           std::make_shared<PersistentCircularQueryBuffer>(id * 10 + 1, (secondSchema == nullptr) ? 0 :
                                                                              m_config ? m_config->getCircularBufferSize() :
                                                                              SystemConf::getInstance().CIRCULAR_BUFFER_SIZE,
                                                                 (secondSchema == nullptr) ? 1 : m_secondSchema->getTupleSize(),
                                                                 copyDataOnInsert, m_config ? m_config->getBatchSize() :
                                                                                   SystemConf::getInstance().BATCH_SIZE, nullptr, clearFiles)),
#endif
      m_dispatcher((secondSchema == nullptr) ?
                   std::shared_ptr<ITaskDispatcher>(std::make_shared<TaskDispatcher>(*this, *m_firstCircularBuffer, replayTimestamps)) :
                   std::make_shared<JoinTaskDispatcher>(*this, *m_firstCircularBuffer, *m_secondCircularBuffer, replayTimestamps)),
      m_resultHandler(std::make_shared<ResultHandler>(*this,
                                                      *m_firstCircularBuffer,
                                                      *m_secondCircularBuffer,
          (secondSchema == nullptr) && hasWindowFragments,
          (secondSchema == nullptr) && useParallelMerge)),
      m_timestampReference(timestampReference),
      m_latencyMonitor(std::make_unique<LatencyMonitor>(m_timestampReference, clearFiles)),
      m_operators(operators),
      m_id(id),
      m_numberOfUpstreamQueries(0),
      m_numberOfDownstreamQueries(0),
      m_upstreamQueries(2, nullptr),
      m_downstreamQueries(2, nullptr),
      m_numOfWindowDefinitions(multipleQueries),
      m_markedForCheckpoint(true) {
  // Merge and re-order operators into a single unit. Set it up accordingly for the pipeline.
  for (auto op: m_operators) {
    op->setParent(this, m_id, m_firstCircularBuffer->getCapacity()/m_firstCircularBuffer->getBatchSize());
    if (op->isMostUpstream())
      m_mostUpstreamOperator = op;
    if (op->isMostDownstream()) {
      m_mostDownstreamOperator = op;
      if (Selection *code = dynamic_cast<Selection *>(&op->getCode())) {
        setOutputSchema(m_firstSchema);
      } else {
        setOutputSchema(&op->getCode().getOutputSchema());
      }
    }
    if (AggregateOperatorCode *code = dynamic_cast<AggregateOperatorCode *>(&op->getCode())) {
      m_resultHandler->setAggregateOperator(code);
    }
  }
  for (int i = 0; i < 2; ++i)
    m_upstreamQueries[i] = m_downstreamQueries[i] = nullptr;

  // set up a reference from the buffer to the query
  // that is used for persistence
  m_firstCircularBuffer->setQuery(
      this, (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON)
                ? m_mostDownstreamOperator->getCode().getInputCols()
                : nullptr);
  if (secondSchema)
    m_secondCircularBuffer->setQuery(this, (SystemConf::getInstance().ADAPTIVE_COMPRESSION_ON)
                                           ? m_mostDownstreamOperator->getCode().getSecondInputCols()
                                           : nullptr);

  if (!SystemConf::getInstance().LATENCY_ON)
    m_latencyMonitor->disable();
}

int Query::getSchemaTupleSize() {
  return m_firstSchema->getTupleSize();
}

void Query::setName(std::string name) {
  m_name = name;
}

void Query::setSQLExpression(std::string sql) {
  m_sql = sql;
}

std::string Query::getName() {
  if (!m_name.empty())
    return m_name;
  else
    return "Query " + std::to_string(m_id);
}

std::string Query::getSQLExpression() {
  return m_sql;
}

int Query::getId() {
  return m_id;
}

bool Query::isMostUpstream() {
  return (m_numberOfUpstreamQueries == 0);
}

bool Query::isMostDownstream() {
  return (m_numberOfDownstreamQueries == 0);
}

QueryConfig *Query::getConfig() {
  return m_config;
}

QueryOperator *Query::getMostUpstreamOperator() {
  return m_mostUpstreamOperator;
}

QueryOperator *Query::getMostDownstreamOperator() {
  return m_mostDownstreamOperator;
}

QueryOperator *Query::getOperator() {
  if (m_operators.size() != 1)
    throw std::runtime_error("error: the query can have only one operator");
  return m_operators[0];
}

QueryApplication *Query::getParent() {
  return m_parent;
}

void Query::setParent(QueryApplication *parent) {
  m_parent = parent;
  m_taskQueue = parent->getTaskQueue();
  m_taskQueueCapacity = parent->getTaskQueueCapacity();
  m_dispatcher->setTaskQueue(m_taskQueue);
}

QueryBuffer *Query::getBuffer() {
  return m_firstCircularBuffer.get();
}

QueryBuffer *Query::getSecondBuffer() {
  return m_secondCircularBuffer.get();
}

std::shared_ptr<ITaskDispatcher> Query::getTaskDispatcher() {
  return m_dispatcher;
}

void Query::setTaskQueue(std::shared_ptr<TaskQueue> &queue) {
  m_taskQueue = queue;
}

std::shared_ptr<TaskQueue> Query::getTaskQueue() {
  return m_taskQueue;
}

size_t Query::getTaskQueueCapacity() {
  return m_taskQueueCapacity;
}

std::shared_ptr<ResultHandler> Query::getResultHandler() {
  return m_resultHandler;
}

void Query::setAggregateOperator(AggregateOperatorCode *aggrOperator) {
  m_resultHandler->setAggregateOperator(aggrOperator);
}

long Query::getBytesGenerated() {
  return m_resultHandler->getTotalOutputBytes();
}

void Query::startDroppingTasks(int task) {
  m_dropTasks = true;
  m_taskToDrop = task;
}

bool Query::isTaskDropped(int task) {
  if (m_dropTasks) {
    return task <= m_taskToDrop;
  }
  return false;
}

long Query::getTimestampReference() {
    return m_latencyMonitor->getTimestampReference();
}

long Query::getLastTimestamp() {
  return m_latencyMonitor->getLastTimestamp();
}

WindowDefinition &Query::getWindowDefinition() {
  return m_firstWindow;
}

TupleSchema *Query::getSchema() {
  return m_firstSchema;
}

WindowDefinition &Query::getFirstWindowDefinition() {
  return m_firstWindow;
}

TupleSchema *Query::getFirstSchema() {
  return m_firstSchema;
}

WindowDefinition &Query::getSecondWindowDefinition() {
  return m_secondWindow;
}

TupleSchema *Query::getSecondSchema() {
  return m_secondSchema;
}

TupleSchema *Query::getOutputSchema() {
  return m_outputSchema;
}

int Query::getNumOfWindowDefinitions() {
  return m_numOfWindowDefinitions;
}

void Query::setOutputSchema(TupleSchema *schema) {
  m_outputSchema = schema;
}

LatencyMonitor &Query::getLatencyMonitor() {
  return *m_latencyMonitor;
}

void Query::connectTo(Query *query) {
  if (m_numberOfDownstreamQueries >= (int) m_downstreamQueries.capacity())
    throw std::out_of_range("error: invalid number of downstream queries in query");

  int idx = m_numberOfDownstreamQueries++;
  m_downstreamQueries[idx] = query;
  query->setUpstreamQuery(this);
}

bool Query::getIsLeft() {
  return m_isLeft;
}

void Query::markForCheckpoint(bool mark) {
  m_markedForCheckpoint = mark;
}

bool Query::isMarkedForCheckpoint() {
  return m_markedForCheckpoint;
}

Query *Query::getUpstreamQuery() {
  return m_upstreamQueries[0];
}

Query *Query::getUpstreamQuery(int idx) {
  return m_upstreamQueries[idx];
}

Query *Query::getDownstreamQuery() {
  return m_downstreamQueries[0];
}

Query *Query::getDownstreamQuery(int idx) {
  return m_downstreamQueries[idx];
}

int Query::getNumberOfUpstreamQueries() {
  return m_numberOfUpstreamQueries;
}

int Query::getNumberOfDownstreamQueries() {
  return m_numberOfDownstreamQueries;
}

void Query::setUpstreamQuery(Query *query) {
  /* If this is the first upstream query that we register, then set it
   * to be the left one (in a two-way join) */
  if (m_numberOfUpstreamQueries == 0)
    query->setLeft(true);

  if (m_numberOfUpstreamQueries >= (int) m_upstreamQueries.capacity())
    throw std::out_of_range("error: invalid number of upstream queries in query");

  int idx = m_numberOfUpstreamQueries++;
  m_upstreamQueries[idx] = query;
}

void Query::setLeft(bool isLeft) {
  m_isLeft = isLeft;
}

Query::~Query() = default;
