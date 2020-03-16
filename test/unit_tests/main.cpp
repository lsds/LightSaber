#include "gtest/gtest.h"

#include "buffers/CircularQueryBuffer.h"
#include "buffers/PartialWindowResultsFactory.h"
#include "buffers/UnboundedQueryBufferFactory.h"
#include "tasks/TaskFactory.h"
#include "tasks/WindowBatchFactory.h"
#include "cql/operators/OperatorCode.h"
#include "cql/operators/NoOp.h"
#include "utils/QueryOperator.h"
#include "utils/QueryApplication.h"
#include "utils/TupleSchema.h"
#include "dispatcher/TaskDispatcher.h"
#include "result/ResultHandler.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/LongConstant.h"
#include "cql/expressions/IntConstant.h"
#include "cql/expressions/operations/Addition.h"
#include "cql/expressions/operations/Multiplication.h"
#include "cql/expressions/operations/Division.h"
#include "cql/expressions/operations/Subtraction.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/operators/Selection.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/Projection.h"
#include "monitors/PerformanceMonitor.h"
#include "monitors/LatencyMonitor.h"

TEST(Expressions, BasicExpressionAndPredicateCreation) {
  ColumnReference ref1(0);
  ColumnReference ref2(3);
  LongConstant const1(2L);
  IntConstant const2(5);
  IntConstant const3(3);
  Addition add(&const2, &const3);
  Multiplication mul(&ref2, &add);
  Division div(&ref1, &const1);
  Subtraction sub(&div, &mul);
  EXPECT_EQ(sub.toSExpr(), "( ( \"0\" / Constant 2 ) - ( \"3\" * ( Constant 5 + Constant 3 ) ) )");

  ColumnReference ref3(1);
  ComparisonPredicate pr1(NONEQUAL_OP, &sub, &ref3);
  EXPECT_EQ(pr1.toSExpr(), "( ( \"0\" / Constant 2 ) - ( \"3\" * ( Constant 5 + Constant 3 ) ) ) != \"1\"");
}

TEST(Selection, OperatorInitialization) {
  Selection selection(new ComparisonPredicate(LESS_OP, new ColumnReference(0), new IntConstant(100)));
  EXPECT_EQ(selection.toSExpr(), "Selection (\"0\" < Constant 100)");
}

TEST(Projection, OperatorInitialization) {
  std::vector<Expression *> expressions(3);
  // Always project the timestamp
  expressions[0] = new ColumnReference(0);
  expressions[1] = new ColumnReference(1);
  expressions[2] = new Division(new Multiplication(new IntConstant(3), new IntConstant(15)), new IntConstant(2));
  Projection projection(expressions);
  EXPECT_EQ(projection.toSExpr(), "Projection (\"0\", \"1\", ( ( Constant 3 * Constant 15 ) / Constant 2 ))");
}

TEST(Aggregation, OperatorInitialization) {
  WindowDefinition windowDefinition(ROW_BASED, 1024, 32);
  std::vector<AggregationType> aggregationTypes(2);
  aggregationTypes[0] = AVG;
  aggregationTypes[1] = MIN;
  std::vector<ColumnReference *> aggregationAttributes(2);
  aggregationAttributes[0] = new ColumnReference(1);
  aggregationAttributes[1] = new ColumnReference(2);
  std::vector<Expression *> groupByAttributes(1);
  groupByAttributes[0] = new ColumnReference(2);
  Aggregation aggregation(windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
  std::cout << aggregation.toSExpr() << std::endl;
  EXPECT_EQ(aggregation.toSExpr(),
            "[Partial window u-aggregation] AVG(\"1\") MIN(\"2\") (group-by ? 1) (incremental ? 1)");
}

TEST(CircularBuffer, ProcessBytes) {
  CircularQueryBuffer circularBuffer(0, 1024, 32);
  std::vector<char> v(127);
  circularBuffer.put(v.data(), v.size());
  circularBuffer.free(127);
  EXPECT_EQ(circularBuffer.getBytesProcessed(), 128);
}

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
      1024, 0, 1024, &query, &buffer, &window, &schema, 0
  );
  EXPECT_EQ(batch->getTaskId(), 0);
  WindowBatchFactory::getInstance().free(batch);
  batch.reset();
}

TEST(TaskDispatcher, CreateTasks) {
  int batchSize = 64 * 1024;
  SystemConf::getInstance().BATCH_SIZE = batchSize;
  std::unique_ptr<WindowDefinition> window = std::make_unique<WindowDefinition>(ROW_BASED, 1, 1);
  std::unique_ptr<TupleSchema> schema = std::make_unique<TupleSchema>(2, "Stream");
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
  long timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = std::make_shared<Query>(0, operators, *window, schema.get(), timestampReference);
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
  delete application;
}

TEST(ResultHandler, FreeSlots) {
  int batchSize = 64 * 1024;
  SystemConf::getInstance().BATCH_SIZE = batchSize;
  std::unique_ptr<WindowDefinition> window = std::make_unique<WindowDefinition>(ROW_BASED, 1, 1);
  std::unique_ptr<TupleSchema> schema = std::make_unique<TupleSchema>(2, "Stream");
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
  long timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = std::make_shared<Query>(0, operators, *window, schema.get(), timestampReference);
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
  delete application;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
