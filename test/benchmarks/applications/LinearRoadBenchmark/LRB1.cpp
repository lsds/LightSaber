#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "cql/expressions/operations/Division.h"
#include "cql/expressions/IntConstant.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "benchmarks/applications/LinearRoadBenchmark/LinearRoadBenchmark.h"

class LRB1 : public LinearRoadBenchmark {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 550; //320;
    //SystemConf::getInstance().HASH_TABLE_SIZE = 256;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    auto segmentExpr = new Division(new ColumnReference(6, BasicType::Integer), new IntConstant(5280));

    // Configure second query
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("avg");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(2, BasicType::Float);

    std::vector<Expression *> groupByAttributes(3);
    groupByAttributes[0] = new ColumnReference(3, BasicType::Integer);
    groupByAttributes[1] = new ColumnReference(5, BasicType::Integer);
    groupByAttributes[2] = segmentExpr;

    auto window = new WindowDefinition(RANGE_BASED, 300, 1); //(ROW_BASED, 300*80, 1*80);
    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    // Configure third query
    auto predicate = new ComparisonPredicate(LESS_OP, new ColumnReference(4), new IntConstant(40));
    Selection *selection = new Selection(predicate);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    genCode->setCollisionBarrier(8);
    genCode->setHaving(selection);
    genCode->setQueryId(0);
    genCode->setup();
    OperatorCode *cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

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
                                         !replayTimestamps,
                                         useParallelMerge);

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

 public:
  LRB1(bool inMemory = true) {
    m_name = "LRB1";
    createSchema();
    createApplication();
    m_fileName = "lrb-data-small-ht.txt";
    if (inMemory)
      loadInMemoryData();
  }
};
