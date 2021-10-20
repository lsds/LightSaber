#include "buffers/CircularQueryBuffer.h"
#include "buffers/PartialWindowResultsFactory.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "compression/CompressionStatistics.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/NoOp.h"
#include "cql/operators/OperatorCode.h"
#include "dispatcher/TaskDispatcher.h"
#include "gtest/gtest.h"
#include "monitors/LatencyMonitor.h"
#include "monitors/PerformanceMonitor.h"
#include "result/ResultHandler.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "utils/QueryApplication.h"
#include "utils/QueryOperator.h"
#include "utils/TupleSchema.h"

TEST(PartialWindowResultsFactory, InitiliazeAndFree) {
  SystemConf::getInstance().PARTIAL_WINDOWS = 1024;
  auto res = PartialWindowResultsFactory::getInstance().newInstance(0);
  res->increment();
  EXPECT_EQ(res->getStartPointer(0), 0);
  PartialWindowResultsFactory::getInstance().free(0, res);
}

TEST(UnboundedQueryBufferFactory, InitiliazeAndFree) {
  auto buffer = UnboundedQueryBufferFactory::getInstance().newInstance();
  EXPECT_EQ(buffer.get()->getBufferId(), 0);
  UnboundedQueryBufferFactory::getInstance().free(buffer);
  buffer = UnboundedQueryBufferFactory::getInstance().newInstance();
  EXPECT_EQ(buffer.get()->getBufferId(), 0);
}

TEST(WindowBatchFactory, InitiliazeAndFree) {
  CircularQueryBuffer buffer(0, 2 * 1024, 32);
  WindowDefinition window(ROW_BASED, 1, 1);
  TupleSchema schema(2, "Stream");
  std::vector<QueryOperator *> operators;
  Query query(0, operators, window, &schema, 0);
  auto batch = WindowBatchFactory::getInstance().newInstance(
      1024, 0, 1024, -1, &query, &buffer, &window, &schema, 0);
  EXPECT_EQ(batch->getTaskId(), 0);
  WindowBatchFactory::getInstance().free(batch);
  batch.reset();
}

TEST(TaskDispatcher, CreateTasks) {
  int batchSize = 64 * 1024;
  SystemConf::getInstance().BATCH_SIZE = batchSize;
  std::unique_ptr<WindowDefinition> window =
      std::make_unique<WindowDefinition>(ROW_BASED, 1, 1);
  std::unique_ptr<TupleSchema> schema =
      std::make_unique<TupleSchema>(2, "Stream");
  int numberOfAttributes = 1;
  for (int i = 1; i < numberOfAttributes + 1; i++) {
    auto attr = AttributeType(BasicType::Long);
    schema->setAttributeType(i, attr);
  }
  auto attr = AttributeType(BasicType::Long);
  schema->setAttributeType(0, attr);
  // Setup the query
  OperatorCode *cpuCode = new NoOp(*schema);
  auto queryOperator = new QueryOperator(*cpuCode);
  std::vector<QueryOperator *> operators;
  operators.push_back(queryOperator);
  long timestampReference =
      std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = std::make_shared<Query>(0, operators, *window, schema.get(),
                                       timestampReference);
  auto application = new QueryApplication(queries);

  queries[0]->setParent(application);

  std::vector<char> input(batchSize);
  auto dispatcher = application->getQueries()[0]->getTaskDispatcher();
  dispatcher->dispatch(input.data(), input.size());
  dispatcher->dispatch(input.data(), input.size());

  std::shared_ptr<Task> task;
  int taskSize = 0;
  taskSize += application->getTaskQueue()->try_dequeue(task);
  taskSize += application->getTaskQueue()->try_dequeue(task);

  EXPECT_EQ(taskSize, 2);
  delete cpuCode;
  delete queryOperator;
  //delete application; // todo: fix this
}

TEST(ResultHandler, FreeSlots) {
  int batchSize = 64 * 1024;
  SystemConf::getInstance().BATCH_SIZE = batchSize;
  std::unique_ptr<WindowDefinition> window =
      std::make_unique<WindowDefinition>(ROW_BASED, 1, 1);
  std::unique_ptr<TupleSchema> schema =
      std::make_unique<TupleSchema>(2, "Stream");
  int numberOfAttributes = 1;
  for (int i = 1; i < numberOfAttributes + 1; i++) {
    auto attr = AttributeType(BasicType::Long);
    schema->setAttributeType(i, attr);
  }
  auto attr = AttributeType(BasicType::Long);
  schema->setAttributeType(0, attr);
  // Setup the query
  OperatorCode *cpuCode = new NoOp(*schema);
  auto queryOperator = new QueryOperator(*cpuCode);
  std::vector<QueryOperator *> operators;
  operators.push_back(queryOperator);
  long timestampReference =
      std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = std::make_shared<Query>(0, operators, *window, schema.get(),
                                       timestampReference);
  auto application = new QueryApplication(queries);

  queries[0]->setParent(application);

  std::vector<char> input(batchSize);
  auto dispatcher = application->getQueries()[0]->getTaskDispatcher();
  dispatcher->dispatch(input.data(), input.size());
  dispatcher->dispatch(input.data(), input.size());

  auto handler = application->getQueries()[0]->getResultHandler();
  std::shared_ptr<Task> task;
  application->getTaskQueue()->try_dequeue(task);
  task->run(0);
  TaskFactory::getInstance().free(task);
  EXPECT_EQ(handler->getTotalOutputBytes(), batchSize);

  application->getTaskQueue()->try_dequeue(task);
  task->run(0);
  TaskFactory::getInstance().free(task);
  EXPECT_EQ(handler->getTotalOutputBytes(), 2 * batchSize);

  delete cpuCode;
  delete queryOperator;
  //delete application; // todo: fix this
}

TEST(CompressionStatistics, addStatistics) {
  // 3 cols: (Timestamp, Int, Float)
  auto cols = new std::vector<ColumnReference*>;
  cols->push_back(new ColumnReference(0, BasicType::Long));
  cols->push_back(new ColumnReference(1, BasicType::Integer));
  cols->push_back(new ColumnReference(2, BasicType::Float));
  CompressionStatistics stats (0, cols);

  // generate data
  struct input {
    long timestamp;
    int _1;
    float _2;
  };
  std::vector<input> data(16);
  for (size_t i = 0; i < 16; ++i) {
    data[i].timestamp = (long)i/6;
    data[i]._1 = 10*(int)i;
    data[i]._2 = ((float) i) * (float) 0.001;
  }

  // gather statistics
  std::vector<uint32_t> m_distinctVals(cols->size());
  std::vector<double> m_consecutiveVals(cols->size(), 1);
  std::vector<double> m_min(cols->size(), DBL_MAX), m_max(cols->size(), DBL_MIN), m_maxDiff(cols->size(), DBL_MIN);
  auto timestamp = data[0].timestamp;
  auto _1 = data[0]._1;
  auto _2 = data[0]._2;
  m_min[0] = timestamp; m_max[0] = timestamp;
  m_min[1] = _1; m_max[1] = _1;
  m_min[2] = _2; m_max[2] = _2;
  for (size_t i = 1; i < 16; ++i) {
    if (timestamp != data[i].timestamp) {
      timestamp = data[i].timestamp;
      m_consecutiveVals[0]++;
    }
    m_min[0] = std::min(m_min[0], (double)data[i].timestamp);
    m_max[0] = std::max(m_max[0], (double)data[i].timestamp);
    m_maxDiff[0] = std::max(m_maxDiff[0], (double)(data[i].timestamp - data[i-1].timestamp));

    if (_1 != data[i]._1) {
      _1 = data[i]._1;
      m_consecutiveVals[1]++;
    }
    m_min[1] = std::min(m_min[1], (double)data[i]._1);
    m_max[1] = std::max(m_max[1], (double)data[i]._1);
    m_maxDiff[1] = std::max(m_maxDiff[1], (double)(data[i]._1 - data[i-1]._1));

    if (_2 != data[i]._2) {
      _2 = data[i]._2;
      m_consecutiveVals[2]++;
    }
    m_min[2] = std::min(m_min[2], (double)data[i]._2);
    m_max[2] = std::max(m_max[2], (double)data[i]._2);
    m_maxDiff[2] = std::max(m_maxDiff[2], (double)(data[i]._2 - data[i-1]._2));
  }
  m_consecutiveVals[0] = 16 / m_consecutiveVals[0];
  m_consecutiveVals[1] = 16 / m_consecutiveVals[1];
  m_consecutiveVals[2] = 16 / m_consecutiveVals[2];

  stats.addStatistics(m_distinctVals.data(), m_consecutiveVals.data(), m_min.data(), m_max.data(), m_maxDiff.data());
  EXPECT_EQ(stats.updateCompressionDecision(), true);

  EXPECT_EQ(stats.m_useRLE[0], true);
  EXPECT_EQ(stats.m_useRLE[1], false);
  EXPECT_EQ(stats.m_useRLE[2], false);
  EXPECT_EQ(stats.m_precision[0], 2);
  EXPECT_EQ(stats.m_precision[1], 8);
  EXPECT_EQ(stats.m_diffPrecision[0], 2);
  EXPECT_EQ(stats.m_diffPrecision[1], 8);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
