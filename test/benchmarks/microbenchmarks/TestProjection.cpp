#include <iostream>

#include "microbenchmarks/RandomDataGenerator.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"

class TestProjection : public RandomDataGenerator {
 private:
  void createApplication() override {

    // Configure projection
    std::vector<Expression *> expressions(2);
    // Always project the timestamp
    expressions[0] = new ColumnReference(0);
    expressions[1] = new ColumnReference(1);
    Projection *projection = new Projection(expressions);

    auto window = new WindowDefinition(ROW_BASED, 60, 60);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setProjection(projection);
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
    queries[0] = std::make_shared<Query>(0, operators, *window, m_schema, timestampReference, false, false, true);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  TestProjection(bool inMemory = true) {
    m_name = "TestProjection";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  benchmarkQuery = new TestProjection();

  return benchmarkQuery->runBenchmark();
}