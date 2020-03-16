#include <iostream>

#include "microbenchmarks/RandomDataGenerator.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/expressions/IntConstant.h"

class TestSelection : public RandomDataGenerator {
 private:
  void createApplication() override {
    // Configure selection
    auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(1), new IntConstant(3));
    Selection *selection = new Selection(predicate);

    auto window = new WindowDefinition(ROW_BASED, 60, 60);

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setSelection(selection);
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
  TestSelection(bool inMemory = true) {
    m_name = "TestSelection";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};

int main(int argc, const char **argv) {
  BenchmarkQuery *benchmarkQuery = nullptr;

  BenchmarkQuery::parseCommandLineArguments(argc, argv);

  benchmarkQuery = new TestSelection();

  return benchmarkQuery->runBenchmark();
}