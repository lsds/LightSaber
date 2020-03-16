#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "utils/Query.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "cql/expressions/IntConstant.h"
#include "benchmarks/applications/YahooBenchmark/YahooBenchmark.h"

class YSB : public YahooBenchmark {
 private:
  TupleSchema *createStaticSchema() {
    if (m_is64)
      return createStaticSchema_64();
    else
      return createStaticSchema_128();
  }

  TupleSchema *createStaticSchema_64() {
    auto staticSchema = new TupleSchema(2, "Campaigns");
    auto longAttr = AttributeType(BasicType::Long);
    //auto longLongAttr = AttributeType(BasicType::LongLong);

    staticSchema->setAttributeType(0, longAttr); /*       ad_id:  long */
    staticSchema->setAttributeType(1, longAttr); /* campaign_id:  long */
    return staticSchema;
  }

  TupleSchema *createStaticSchema_128() {
    auto staticSchema = new TupleSchema(2, "Campaigns");
    auto longLongAttr = AttributeType(BasicType::LongLong);

    staticSchema->setAttributeType(0, longLongAttr); /*       ad_id:  longLong */
    staticSchema->setAttributeType(1, longLongAttr); /* campaign_id:  longLong */
    return staticSchema;
  }

  std::string getStaticHashTable() {
    std::string s;
    std::string type;
    if (m_is64)
      type = "long";
    else
      type = "__uint128_t";
    s.append(
        "\n"
        "struct interm_node {\n"
        "    long timestamp;\n"
        "    " + type + " ad_id;\n"
                        "    " + type + " campaign_id;\n"
                                        "};\n"
                                        "struct static_node {\n"
                                        "    " + type + " key;\n"
                                                        "    " + type + " value;\n"
                                                                        "};\n"
                                                                        "class staticHashTable {\n"
                                                                        "private:\n"
                                                                        "    int size = 1024;\n"
                                                                        "    int mask = 1024-1;\n"
                                                                        "    static_node *table;\n");

    if (m_is64)
      s.append("    std::hash<long> hashVal;\n");
    else
      s.append("    MyHash hashVal\n;");

    s.append(
        "public:\n"
        "    staticHashTable (static_node *table);\n"
        "    bool get_value (const " + type + " key, " + type + " &result);\n"
                                                                "};\n"
                                                                "staticHashTable::staticHashTable (static_node *table) {\n"
                                                                "    this->table = table;\n"
                                                                "}\n"
                                                                "bool staticHashTable::get_value (const " + type
            + " key, " + type + " &result) {\n"
                                "    int ind = hashVal(key) & mask;\n"
                                "    int i = ind;\n"
                                "    for (; i < this->size; i++) {\n"
                                "        if (this->table[i].key == key) {\n"
                                "            result = this->table[i].value;\n"
                                "            return true;\n"
                                "        }\n"
                                "    }\n"
                                "    for (i = 0; i < ind; i++) {\n"
                                "        if (this->table[i].key == key) {\n"
                                "            result = this->table[i].value;\n"
                                "            return true;\n"
                                "        }\n"
                                "    }\n"
                                "    return false;\n"
                                "}\n\n"
    );
    return s;
  }

  std::string getStaticComputation(WindowDefinition *window) {
    std::string s;
    if (m_is64) {
      if (window->isRowBased())
        s.append("if (data[bufferPtr]._5 == 0) {\n");

      s.append("    long joinRes;\n");
      s.append(
          "    bool joinFound = staticMap.get_value(data[bufferPtr]._3, joinRes);\n"
          "    if (joinFound) {\n"
          "        interm_node tempNode = {data[bufferPtr].timestamp, data[bufferPtr]._3, joinRes};\n"
          "        curVal._1 = 1;\n"
          "        curVal._2 = tempNode.timestamp;\n"
          "        aggrStructures[pid].insert_or_modify(tempNode.campaign_id, curVal, tempNode.timestamp);\n"
          "    }\n");
      if (window->isRowBased())
        s.append("}\n");
    } else {
      if (window->isRowBased())
        s.append("if (data[bufferPtr]._6 == 0) {\n");

      s.append("    __uint128_t joinRes;\n");
      s.append(
          "    bool joinFound = staticMap.get_value(data[bufferPtr]._4, joinRes);\n"
          "    if (joinFound) {\n"
          "        interm_node tempNode = {data[bufferPtr].timestamp, data[bufferPtr]._4, joinRes};\n"
          "        curVal._1 = 1;\n"
          "        curVal._2 = tempNode.timestamp;\n"
          "        aggrStructures[pid].insert_or_modify(tempNode.campaign_id, curVal, tempNode.timestamp);\n"
          "    }\n");
      if (window->isRowBased())
        s.append("}\n");
    }
    return s;
  }

  std::string getStaticInitialization() {
    std::string s;
    s.append(
        "static_node *sBuf = (static_node *) staticBuffer;\n"
        "staticHashTable staticMap (sBuf);\n"
    );
    return s;
  }

  void createApplication() override {
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 32;
    SystemConf::getInstance().HASH_TABLE_SIZE = 128;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    int incr = (m_is64) ? 0 : 1;

    auto window = new WindowDefinition(RANGE_BASED, 100, 100);

    // Configure selection predicate
    auto predicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(5 + incr), new IntConstant(0));
    Selection *selection = new Selection(predicate);

    // Configure projection
    std::vector<Expression *> expressions(2);
    // Always project the timestamp
    expressions[0] = new ColumnReference(0);
    expressions[1] = new ColumnReference(3 + incr);
    Projection *projection = new Projection(expressions, true);

    // Configure static hashjoin
    auto staticSchema = createStaticSchema();
    auto joinPredicate = new ComparisonPredicate(EQUAL_OP, new ColumnReference(1), new ColumnReference(0));
    StaticHashJoin *staticJoin = new StaticHashJoin(joinPredicate,
                                                    projection->getOutputSchema(),
                                                    *staticSchema,
                                                    getStaticData(),
                                                    getStaticInitialization(),
                                                    getStaticHashTable(),
                                                    getStaticComputation(window));

    // Configure aggregation
    std::vector<AggregationType> aggregationTypes(2);
    aggregationTypes[0] = AggregationTypes::fromString("cnt");
    aggregationTypes[1] = AggregationTypes::fromString("max");

    std::vector<ColumnReference *> aggregationAttributes(2);
    aggregationAttributes[0] = new ColumnReference(1 + incr, BasicType::Float);
    aggregationAttributes[1] = new ColumnReference(0, BasicType::Float);

    std::vector<Expression *> groupByAttributes(1);
    if (m_is64)
      groupByAttributes[0] = new ColumnReference(3, BasicType::Long);
    else
      groupByAttributes[0] = new ColumnReference(4, BasicType::LongLong);

    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

    bool replayTimestamps = window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge, true);
    genCode->setInputSchema(getSchema());
    genCode->setSelection(selection);
    //genCode->setProjection(projection);
    genCode->setStaticHashJoin(staticJoin);
    genCode->setAggregation(aggregation);
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
  YSB(bool inMemory = true) {
    m_name = "YSB";
    createSchema();
    if (inMemory)
      loadInMemoryData();
    createApplication();
  }
};