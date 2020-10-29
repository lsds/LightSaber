#include "utils/Query.h"
#include "utils/QueryApplication.h"
#include "utils/TupleSchema.h"
#include "utils/QueryOperator.h"
#include "dispatcher/TaskDispatcher.h"
#include "tasks/Task.h"
#include "result/ResultHandler.h"
#include "monitors/LatencyMonitor.h"
#include "cql/operators/AggregateOperatorCode.h"
#include "cql/operators/Selection.h"
#if defined(HAVE_NUMA)
#include "buffers/NUMACircularQueryBuffer.h"
#else
#include "buffers/CircularQueryBuffer.h"
#endif

#include <memory>

Query::Query(int id, std::vector<QueryOperator *> &operators, WindowDefinition window,
             TupleSchema *schema, long timestampReference, bool hasWindowFragments, bool replayTimestamps,
             bool copyDataOnInsert, bool useParallelMerge, int multipleQueries)
    : m_window(window),
      m_schema(schema),
#if defined(HAVE_NUMA)
    m_circularBuffer(std::make_shared<NUMACircularQueryBuffer>(id, SystemConf::getInstance().CIRCULAR_BUFFER_SIZE, schema->getTupleSize(), copyDataOnInsert)),
#else
      m_circularBuffer(std::make_shared<CircularQueryBuffer>(id,
                                                             SystemConf::getInstance().CIRCULAR_BUFFER_SIZE,
                                                             schema->getTupleSize(),
                                                             copyDataOnInsert)),
#endif
      m_dispatcher(std::make_shared<TaskDispatcher>(*this,
                                                    *m_circularBuffer,
                                                    replayTimestamps)),
      m_resultHandler(std::make_shared<ResultHandler>(*this,
                                                      *m_circularBuffer,
                                                      hasWindowFragments,
                                                      useParallelMerge)),
      m_timestampReference(timestampReference),
      m_latencyMonitor(std::make_unique<LatencyMonitor>(m_timestampReference)),
      m_operators(operators),
      m_id(id),
      m_numberOfUpstreamQueries(0),
      m_numberOfDownstreamQueries(0),
      m_upstreamQueries(2),
      m_downstreamQueries(2),
      m_numOfWindowDefinitions(multipleQueries) {

  // Merge and re-order operators into a single unit. Set it up accordingly for the pipeline.
  for (auto op: m_operators) {
    op->setParent(this);
    if (op->isMostUpstream())
      m_mostUpstreamOperator = op;
    if (op->isMostDownstream()) {
      m_mostDownstreamOperator = op;
      if (Selection *code = dynamic_cast<Selection *>(&op->getCode())) {
        setOutputSchema(schema);
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

  if (!SystemConf::getInstance().LATENCY_ON)
    m_latencyMonitor->disable();
}

int Query::getSchemaTupleSize() {
  return m_schema->getTupleSize();
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

QueryOperator *Query::getMostUpstreamOperator() {
  return m_mostUpstreamOperator;
}

QueryOperator *Query::getMostDownstreamOperator() {
  return m_mostDownstreamOperator;
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
  return m_circularBuffer.get();
}

std::shared_ptr<TaskDispatcher> Query::getTaskDispatcher() {
  return m_dispatcher;
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

WindowDefinition &Query::getWindowDefinition() {
  return m_window;
}

TupleSchema *Query::getSchema() {
  return m_schema;
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
