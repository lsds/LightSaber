#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "benchmarks/applications/ManufacturingEquipment/ManufacturingEquipment.h"

class ME1 : public ManufacturingEquipment {
 private:
  void createApplication() override {
    SystemConf::getInstance().UNBOUNDED_BUFFER_SIZE = 4096;
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 128; // change this depending on the batch size

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    std::vector<AggregationType> aggregationTypes(3);
    aggregationTypes[0] = AggregationTypes::fromString("avg");
    aggregationTypes[1] = AggregationTypes::fromString("avg");
    aggregationTypes[2] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(3);
    aggregationAttributes[0] = new ColumnReference(2, BasicType::Integer);
    aggregationAttributes[1] = new ColumnReference(3, BasicType::Integer);
    aggregationAttributes[2] = new ColumnReference(4, BasicType::Integer);

    std::vector<Expression *> groupByAttributes;

    auto window = new WindowDefinition(RANGE_BASED, 60, 1);

    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    // reuse previous generated file
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge, true);
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

    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();

    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0, operators, *window, m_schema, m_timestampReference,
                                         true, replayTimestamps, !replayTimestamps, useParallelMerge);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  ME1(bool inMemory = true, bool startApp = true) {
    m_name = "ME1";
    createSchema();
    if (startApp)
      createApplication();
    if (inMemory)
      loadInMemoryData();
  }
};
