#include <iostream>

#include "microbenchmarks/RandomDataGenerator.h"
#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"

class TestAggregation : public RandomDataGenerator {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 256;
    SystemConf::getInstance().PARTIAL_WINDOWS = 64; // change this depending on the batch size

    // Configure non-grouped aggregation. Check the application benchmarks for grouped aggreagations.
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("sum");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes(0);

    auto window = new WindowDefinition(ROW_BASED, 1000, 1000);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    auto queryOperator = new QueryOperator(*cpuCode);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    long timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0, operators, *window, m_schema, timestampReference, true, false, true);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  TestAggregation(bool inMemory = true) {
    m_name = "TestAggregation";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  benchmarkQuery = new TestAggregation();

  return benchmarkQuery->runBenchmark();
}