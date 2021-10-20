#include <cql/expressions/IntConstant.h>

#include "cql/expressions/ColumnReference.h"
#include "cql/operators/ThetaJoin.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "microbenchmarks/RandomDataGenerator.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

class TestJoin : public RandomDataGenerator {
 private:
  void createApplication() override {
    SystemConf::getInstance().BATCH_SIZE = 256 * SystemConf::getInstance()._KB;
    SystemConf::getInstance().BUNDLE_SIZE = 4 * SystemConf::getInstance()._KB;
    SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = 2 * SystemConf::getInstance()._MB;
    //SystemConf::getInstance().SLOTS = 16 * 1024;
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 256 * 1048576;

    long windowSize = 1024;
    long windowSlide = 1024;
    bool copyDataOnInsert = false;

    int numberOfAttributes = 2;
    auto longAttr = AttributeType(BasicType::Long);
    auto intAttr = AttributeType(BasicType::Integer);

    auto window1 = new WindowDefinition(ROW_BASED, windowSize, windowSlide);
    auto schema1 = new TupleSchema(numberOfAttributes + 1, "Stream1");
    for (int i = 1; i < numberOfAttributes + 1; i++) {
      schema1->setAttributeType(i, intAttr);
    }
    schema1->setAttributeType(0, longAttr);

    auto window2 = new WindowDefinition(ROW_BASED, windowSize, windowSlide);
    auto schema2 = new TupleSchema(numberOfAttributes + 1, "Stream2");
    for (int i = 1; i < numberOfAttributes + 1; i++) {
      schema2->setAttributeType(i, intAttr);
    }
    schema2->setAttributeType(0, longAttr);

    auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(1), new ColumnReference(1));
    auto join = new ThetaJoin(*schema1, *schema2, predicate);
    join->setQueryId(0);
    join->setup(window1, window2);

    // set up application
    auto queryOperator = new QueryOperator(*join);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window1,
                                         schema1,
                                         *window2,
                                         schema2,
                                         m_timestampReference,
                                         true,
                                         false,
                                         copyDataOnInsert,
                                         false);

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON);
    m_application->setup();
  }

 public:
  TestJoin(bool inMemory = true) {
    m_name = "TestJoin";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};

int main(int argc, const char **argv) {
  std::unique_ptr<BenchmarkQuery> benchmarkQuery {};

  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  benchmarkQuery = std::make_unique<TestJoin>();

  return benchmarkQuery->runTwoStreamsBenchmark();
}