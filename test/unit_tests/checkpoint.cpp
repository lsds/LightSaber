#include <unistd.h>

#include <memory>
#include <random>

#include "checkpoint/FileBackedCheckpointCoordinator.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "dispatcher/TaskDispatcher.h"
#include "filesystem/File.h"
#include "filesystem/FileSystemDisk.h"
#include "filesystem/NullDisk.h"
#include "gtest/gtest.h"
#include "result/PartialResultSlot.h"
#include "result/ResultHandler.h"
#include "unit_tests/utils/AsyncCircularQueryBuffer.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/SystemConf.h"
#include "utils/Utils.h"

#define DISK
//#undef DISK
#define NUM_OF_WORKERS 1

typedef QueueIoHandler f_handler_t;
typedef FileSystemDisk<f_handler_t> f_disk_t;
typedef NullHandler nd_handler_t;
typedef NullDisk nd_disk_t;

// helper functions definition
TupleSchema createSchema();
Aggregation *createAggregation();
std::shared_ptr<Query> createQuery();

TEST(Checkpoint, CreateAndDeleteCheckpointFiles) {
  // Create a single pipeline
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = createQuery();

#ifdef DISK
  FileBackedCheckpointCoordinator coordinator(0, queries);
#else
  CheckpointCoordinator<nd_disk_t, nd_handler_t> coordinator(0, queries);
#endif

  coordinator.clearPersistentMemory();
}

TEST(Checkpoint, EmulateCheckpointWrites) {
  SystemConf::getInstance().SLOTS = 256;
  SystemConf::getInstance().PARTIAL_WINDOWS = 512;
  // SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = 4 *
  // SystemConf::getInstance()._4MB;
  SystemConf::getInstance().HASH_TABLE_SIZE = 8;
  SystemConf::getInstance().CHECKPOINT_INTERVAL = 1000L;  // msec
  SystemConf::getInstance().BLOCK_SIZE = SystemConf::_4MB;
  SystemConf::getInstance().DURATION = 10;

  // Create a single pipeline
  std::vector<std::shared_ptr<Query>> queries(1);
  queries[0] = createQuery();

  auto filesystem = std::make_shared<f_disk_t>(SystemConf::FILE_ROOT_PATH);
  //FileBackedCheckpointCoordinator coordinator(0, queries, filesystem, false);

  auto dispatcher = queries[0]->getTaskDispatcher();
  auto resultHandler = queries[0]->getResultHandler();

  // fill with dummy data the window fragments
  auto &slots = resultHandler->getPartials();
  for (auto &s : slots) {
    s.m_taskId = dispatcher->getTaskNumber();
    s.m_slot.store(1);  // set the slot as ready for checkpoint
    s.m_openingWindows = PartialWindowResultsFactory::getInstance().newInstance(
        0, SystemConf::getInstance().HASH_TABLE_SIZE);
    s.m_openingWindows->incrementCount(60);
    s.m_closingWindows = PartialWindowResultsFactory::getInstance().newInstance(
        0, SystemConf::getInstance().HASH_TABLE_SIZE);
    s.m_closingWindows->incrementCount(60);
    s.m_completeWindows =
        PartialWindowResultsFactory::getInstance().newInstance(0);
    s.m_completeWindows->incrementCount(220);
    s.m_completeWindows->setPosition(
        220 * SystemConf::getInstance().HASH_TABLE_SIZE *
        queries[0]->getOperator()->getCode().getOutputSchema().getTupleSize());
  }

  //std::thread checkpointThread = std::thread(std::ref(coordinator));
  //checkpointThread.detach();

  const long m_duration = SystemConf::getInstance().DURATION + 2;
  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span{};
  while (true) {
    auto t2 = std::chrono::high_resolution_clock::now();
    time_span =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    if (time_span.count() >= (double)m_duration) {
      std::cout << "Master is stopping..." << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      break;
    }
  }

  //coordinator.clearPersistentMemory();
}

// helper functions implementation
TupleSchema createSchema() {
  TupleSchema schema(3, "Stream");
  for (int i = 1; i < schema.numberOfAttributes() + 1; i++) {
    auto attr = AttributeType(BasicType::Integer);
    schema.setAttributeType(i, attr);
  }
  auto attr = AttributeType(BasicType::Long);
  schema.setAttributeType(0, attr);
  return schema;
}

Aggregation *createAggregation() {
  std::vector<AggregationType> aggregationTypes(1);
  aggregationTypes[0] = AggregationTypes::fromString("sum");
  std::vector<ColumnReference *> aggregationAttributes(1);
  aggregationAttributes[0] = new ColumnReference(1, BasicType::Integer);
  std::vector<Expression *> groupByAttributes(1);
  groupByAttributes[0] = new ColumnReference(2, BasicType::Integer);
  auto window = new WindowDefinition(RANGE_BASED, 60, 1);
  return new Aggregation(*window, aggregationTypes, aggregationAttributes,
                         groupByAttributes);
}

std::shared_ptr<Query> createQuery() {
  auto schema = createSchema();
  auto aggregation = createAggregation();
  OperatorCode *cpuCode = aggregation;
  auto queryOperator = new QueryOperator(*cpuCode);
  std::vector<QueryOperator *> operators;
  operators.push_back(queryOperator);
  return std::make_shared<Query>(0, operators,
                                 aggregation->getWindowDefinition(), &schema, 0,
                                 true, false, true, false);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
