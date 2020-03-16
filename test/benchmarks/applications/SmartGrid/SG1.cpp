#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "benchmarks/applications/SmartGrid/SmartGrid.h"

class SG1 : public SmartGrid {
 private:
  void createApplication() override {
    SystemConf::getInstance().PARTIAL_WINDOWS = 3800;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes;

    auto window = new WindowDefinition(RANGE_BASED, 3600, 1); //ROW_BASED, 85*400, 1*400);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << genCode->toSExpr() << std::endl;

    auto queryOperator = new QueryOperator(*cpuCode);
    std::vector<QueryOperator *> operators;
    operators.push_back(queryOperator);

    // this is used for latency measurements
    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         operators,
                                         *window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         !replayTimestamps);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  SG1(bool inMemory = true) {
    m_name = "SG1";
    createSchema();
    createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
