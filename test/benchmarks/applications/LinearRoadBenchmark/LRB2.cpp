#include "cql/operators/AggregationType.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/operations/Division.h"
#include "utils/WindowDefinition.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "utils/QueryOperator.h"
#include "cql/expressions/IntConstant.h"
#include "utils/Query.h"
#include "benchmarks/applications/LinearRoadBenchmark/LinearRoadBenchmark.h"

class LRB2 : public LinearRoadBenchmark {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 512;
    SystemConf::getInstance().PARTIAL_WINDOWS = 544;
    SystemConf::getInstance().HASH_TABLE_SIZE = 2 * 1024;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;

    // Configure first query
    auto segmentExpr = new Division(new ColumnReference(6, BasicType::Integer), new IntConstant(5280));

    // Configure second query
    std::vector<AggregationType> _aggregationTypes(1);
    _aggregationTypes[0] = AggregationTypes::fromString("cnt");

    std::vector<ColumnReference *> _aggregationAttributes(1);
    _aggregationAttributes[0] = new ColumnReference(2, BasicType::Float);

    std::vector<Expression *> _groupByAttributes(4);
    _groupByAttributes[0] = new ColumnReference(1, BasicType::Integer);
    _groupByAttributes[1] = new ColumnReference(3, BasicType::Integer);
    _groupByAttributes[2] = new ColumnReference(5, BasicType::Integer);
    _groupByAttributes[3] = segmentExpr;

    auto _window = new WindowDefinition(RANGE_BASED, 30, 1); //(ROW_BASED, 30*1000, 1*1000);
    Aggregation
        *_aggregation = new Aggregation(*_window, _aggregationTypes, _aggregationAttributes, _groupByAttributes);

    bool replayTimestamps = _window->isRangeBased();

    // Set up code-generated operator
    OperatorKernel *_genCode = new OperatorKernel(true, true, useParallelMerge);
    _genCode->setInputSchema(getSchema());
    _genCode->setAggregation(_aggregation);
    _genCode->setCustomHashTable(buildCustomHashTable());
    _genCode->setQueryId(0);
    _genCode->setup();
    OperatorCode *_cpuCode = _genCode;

    // Print operator
    std::cout << _cpuCode->toSExpr() << std::endl;
    auto _queryOperator = new QueryOperator(*_cpuCode);
    std::vector<QueryOperator *> _operators;
    _operators.push_back(_queryOperator);

    m_timestampReference = std::chrono::system_clock::now().time_since_epoch().count();
    std::vector<std::shared_ptr<Query>> queries(1);
    queries[0] = std::make_shared<Query>(0,
                                         _operators,
                                         *_window,
                                         m_schema,
                                         m_timestampReference,
                                         true,
                                         replayTimestamps,
                                         false/*!replayTimestamps*/,
                                         useParallelMerge);

    SystemConf::getInstance().SLOTS = 512;
    SystemConf::getInstance().PARTIAL_WINDOWS = 512; // Change this according to the previous operator
    SystemConf::getInstance().HASH_TABLE_SIZE = 256;
    // change the CIRCULAR_BUFFER_SIZE value back for pipelining
    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = 2 * SystemConf::getInstance().CIRCULAR_BUFFER_SIZE;

    // Configure third query
    std::vector<AggregationType> aggregationTypes_(1);
    aggregationTypes_[0] = AggregationTypes::fromString("cnt");

    std::vector<ColumnReference *> aggregationAttributes_(1);
    aggregationAttributes_[0] = new ColumnReference(2, BasicType::Float);

    std::vector<Expression *> groupByAttributes_(3);
    groupByAttributes_[0] = new ColumnReference(2, BasicType::Integer);
    groupByAttributes_[1] = new ColumnReference(3, BasicType::Integer);
    groupByAttributes_[2] = new ColumnReference(4, BasicType::Integer);

    auto window_ = new WindowDefinition(ROW_BASED, 1024, 1024);
    Aggregation
        *aggregation_ = new Aggregation(*window_, aggregationTypes_, aggregationAttributes_, groupByAttributes_);

    TupleSchema *schema_ = &(((OperatorKernel *) _cpuCode)->getOutputSchema());

    // Set up code-generated operator
    OperatorKernel *genCode_ = new OperatorKernel(true);
    genCode_->setInputSchema(schema_);
    genCode_->setAggregation(aggregation_);
    genCode_->setQueryId(1);
    //genCode_->setup(); // uncomment the following and line 105-106 for pipelining the two operations
    OperatorCode *cpuCode_ = genCode_;

    auto queryOperator_ = new QueryOperator(*cpuCode_);
    std::vector<QueryOperator *> operators_;
    operators_.push_back(queryOperator_);

    //queries[1] = std::make_shared<Query>(1, operators_, *window_, schema_, timestampReference, true, false, true);
    //queries[0]->connectTo(queries[1].get());

    SystemConf::getInstance().CIRCULAR_BUFFER_SIZE = SystemConf::getInstance().CIRCULAR_BUFFER_SIZE / 2;

    m_application = new QueryApplication(queries);
    m_application->setup();
  }

  std::string buildCustomHashTable() {
    std::string barrier = (SystemConf::getInstance().PARALLEL_MERGE_ON) ? "8" : "220";
    return
        "struct Key {\n"
        "    int _0;\n"
        "    int _1;\n"
        "    int _2;\n"
        "    int _3;\n"
        "};\n"
        "using KeyT = Key;\n"
        "using ValueT = Value;\n"
        "\n"
        "struct HashMapEqualTo {\n"
        "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
        "        return lhs._0 == rhs._0 && lhs._1 == rhs._1 && lhs._2 == rhs._2 && lhs._3 == rhs._3;\n"
        "    }\n"
        "};\n"
        "\n"
        "struct CustomHash {\n"
        "    std::size_t operator()(KeyT t) const {\n"
        "        std::hash<int> _h;\n"
        "        return _h(t._0);\n"
        "    }\n"
        "};\n"
        "using MyHash = CustomHash;\n"
        "\n"
        "struct alignas(16) Bucket {\n"
        "    char state;\n"
        "    char dirty;\n"
        "    long timestamp;\n"
        "    KeyT key;\n"
        "    ValueT value;\n"
        "    int counter;\n"
        "};\n"
        "\n"
        "using BucketT = Bucket;\n"
        "\n"
        "class alignas(64) HashTable {\n"
        "private:\n"
        "    using HashT = MyHash; //std::hash<KeyT>;\n"
        "    using EqT = HashMapEqualTo;\n"
        "    using AggrT = Aggregator;\n"
        "\n"
        "    HashT     _hasher;\n"
        "    EqT       _eq;\n"
        "    BucketT*  _buckets     = nullptr;\n"
        "    AggrT*    _aggrs       = nullptr;\n"
        "    size_t    _num_buckets = MAP_SIZE;\n"
        "    size_t    _num_filled  = 0;\n"
        "    size_t    _mask        = MAP_SIZE-1;\n"
        "    int       _barrier     = " + barrier + ";\n"
                                                    "public:\n"
                                                    "    HashTable ();\n"
                                                    "    HashTable (Bucket*nodes);\n"
                                                    "    void init ();\n"
                                                    "    void reset ();\n"
                                                    "    void clear ();\n"
                                                    "    void insert (KeyT &key, ValueT &value, long timestamp);\n"
                                                    "    void insert_or_modify (KeyT &key, ValueT &value, long timestamp);\n"
                                                    "    bool evict (KeyT &key);\n"
                                                    "    void insertSlices ();\n"
                                                    "    void evictSlices ();\n"
                                                    "    void setValues ();\n"
                                                    "    void setIntermValues (int pos);\n"
                                                    "    bool get_value (const KeyT &key, ValueT &result);\n"
                                                    "    bool get_result (const KeyT &key, ValueT &result);\n"
                                                    "    bool get_index (const KeyT &key, int &index);\n"
                                                    "    void deleteHashTable();\n"
                                                    "    BucketT* getBuckets ();\n"
                                                    "    size_t getSize() const;\n"
                                                    "    bool isEmpty() const;\n"
                                                    "    size_t getNumberOfBuckets() const;\n"
                                                    "    float load_factor() const;\n"
                                                    "};\n"
                                                    "\n"
                                                    "HashTable::HashTable () {}\n"
                                                    "\n"
                                                    "HashTable::HashTable (Bucket *nodes) : _buckets(nodes) {\n"
                                                    "    if (!(_num_buckets && !(_num_buckets & (_num_buckets - 1)))) {\n"
                                                    "        throw std::runtime_error (\"error: the size of the hash table has to be a power of two\\n\");\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::init () {\n"
                                                    "    if (!(_num_buckets && !(_num_buckets & (_num_buckets - 1)))) {\n"
                                                    "        throw std::runtime_error (\"error: the size of the hash table has to be a power of two\\n\");\n"
                                                    "    }\n"
                                                    "\n"
                                                    "    _buckets  = (BucketT*)malloc(_num_buckets * sizeof(BucketT));\n"
                                                    "    _aggrs  = (AggrT*)malloc(_num_buckets * sizeof(AggrT));\n"
                                                    "    if (!_buckets /*|| !_aggrs*/) {\n"
                                                    "        free(_buckets);\n"
                                                    "        /*free(_aggrs);*/\n"
                                                    "        throw std::bad_alloc();\n"
                                                    "    }\n"
                                                    "\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        _buckets[i].state = 0;\n"
                                                    "        _buckets[i].dirty = 0;\n"
                                                    "        _aggrs[i] = AggrT (); // maybe initiliaze this on insert\n"
                                                    "        _aggrs[i].initialise();\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::reset () {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        _buckets[i].state = 0;\n"
                                                    "        //_aggrs[i].initialise();\n"
                                                    "    }\n"
                                                    "    _num_filled = 0;\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::clear () {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        _buckets[i].state = 0;\n"
                                                    "        _buckets[i].dirty = 0;\n"
                                                    "        //_buckets[i].counter = 0;\n"
                                                    "        _aggrs[i].initialise();\n"
                                                    "    }\n"
                                                    "    _num_filled = 0;\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::insert (KeyT &key, ValueT &value, long timestamp) {\n"
                                                    "    size_t ind = _hasher(key) & _mask, i = ind;\n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        if (!_buckets[i].state || _eq(_buckets[i].key, key)) {\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key; //std::memcpy(&_buckets[i].key, key, KEY_SIZE);\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        if (!_buckets[i].state || _eq(_buckets[i].key, key)) {\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key;\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    throw std::runtime_error (\"error: the hashtable is full \\n\");\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::insert_or_modify (KeyT &key, ValueT &value, long timestamp) {\n"
                                                    "    size_t ind = _hasher(key) & _mask, i = ind;\n"
                                                    "    char tempState;\n"
                                                    "    int steps = 0;\n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        tempState = _buckets[i].state;\n"
                                                    "        if (tempState && _eq(_buckets[i].key, key)) {\n"
                                                    "\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
                                                    "            _buckets[i].counter++;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        if (!tempState && (_buckets[i].key._0 == key._0 || _eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].dirty = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key;\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "            _aggrs[i].initialise();\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            printf(\"Too many collisions, increase the size...\\n\");\n"
                                                    "            exit(1);\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        tempState = _buckets[i].state;\n"
                                                    "        if (tempState && _eq(_buckets[i].key, key)) {\n"
                                                    "\t\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
                                                    "            _buckets[i].counter++;\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        if (!tempState && (_buckets[i].key._0 == key._0 || _eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "            _buckets[i].dirty = 1;\n"
                                                    "            _buckets[i].timestamp = timestamp;\n"
                                                    "            _buckets[i].key = key;\n"
                                                    "            _buckets[i].value = value;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "            _aggrs[i].initialise();\n"
                                                    "            return;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            printf(\"Too many collisions, increase the size...\\n\");\n"
                                                    "            exit(1);\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    throw std::runtime_error (\"error: the hashtable is full \\n\");\n"
                                                    "}\n"
                                                    "\n"
                                                    "bool HashTable::evict (KeyT &key) {\n"
                                                    "    size_t ind = _hasher(key) & _mask, i = ind;\n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            _buckets[i].state = 0;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            _buckets[i].state = 0;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    printf (\"error: entry not found \\n\");\n"
                                                    "    return false;\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::insertSlices () {\n"
                                                    "    int maxNumOfSlices = INT_MIN;\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        int temp = _aggrs[i].addedElements - _aggrs[i].removedElements;\n"
                                                    "        if (_buckets[i].state) {\n"
                                                    "                node n;\n"
                                                    "\t\t\t\tn._1 = _buckets[i].value._1;\n"
                                                    "                _aggrs[i].insert(n);\n"
                                                    "            _buckets[i].state = 0;\n"
                                                    "            //_buckets[i].value = ValueT();\n"
                                                    "        } else if (temp > 0) {\n"
                                                    "            ValueT val;\n"
                                                    "            node n;\n"
                                                    "\t\t\tn._1 = val._1;\n"
                                                    "            _aggrs[i].insert(n);\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::evictSlices () {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
                                                    "            _aggrs[i].evict();\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::setValues () {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
                                                    "            auto res = _aggrs[i].query();\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "\t\t\t_buckets[i].value._1 = res._1;\n"
                                                    "            _buckets[i].counter = 1;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::setIntermValues (int pos) {\n"
                                                    "    for (auto i = 0; i < _num_buckets; ++i) {\n"
                                                    "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
                                                    "            auto res = _aggrs[i].queryIntermediate (pos);\n"
                                                    "            _buckets[i].state = 1;\n"
                                                    "\t\t\t_buckets[i].value._1 = res._1;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "}\n"
                                                    "\n"
                                                    "bool HashTable::get_value (const KeyT &key, ValueT &result) {\n"
                                                    "    size_t ind = _hasher(key) & _mask, i = ind;\n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            result = _buckets[i].value;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            result = _buckets[i].value;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "    }\n"
                                                    "    return false;\n"
                                                    "}\n"
                                                    "\n"
                                                    "bool HashTable::get_index (const KeyT &key, int &index) {\n"
                                                    "    size_t ind = _hasher(key) & _mask, i = ind;\n"
                                                    "    int steps = 0;\n"
                                                    "    index = -1; \n"
                                                    "    for (; i < _num_buckets; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            index = i;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "        if (_buckets[i].state == 0 && index == -1) {\n"
                                                    "            index = i;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            return false;\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    for (i = 0; i < ind; i++) {\n"
                                                    "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
                                                    "            index = i;\n"
                                                    "            return true;\n"
                                                    "        }\n"
                                                    "        if (_buckets[i].state == 0 && index == -1) {\n"
                                                    "            index = i;\n"
                                                    "        }\n"
                                                    "        steps++;\n"
                                                    "        if (steps == _barrier ) {\n"
                                                    "            return false;\n"
                                                    "        };\n"
                                                    "    }\n"
                                                    "    return false;\n"
                                                    "}\n"
                                                    "\n"
                                                    "void HashTable::deleteHashTable() {\n"
                                                    "    for (size_t bucket=0; bucket<_num_buckets; ++bucket) {\n"
                                                    "        _buckets[bucket].~BucketT();\n"
                                                    "        _aggrs->~AggrT();\n"
                                                    "    }\n"
                                                    "    free(_buckets);\n"
                                                    "    free(_aggrs);\n"
                                                    "}\n"
                                                    "\n"
                                                    "BucketT* HashTable::getBuckets () {\n"
                                                    "    return _buckets;\n"
                                                    "}\n"
                                                    "\n"
                                                    "size_t HashTable::getSize() const {\n"
                                                    "    return _num_filled;\n"
                                                    "}\n"
                                                    "\n"
                                                    "bool HashTable::isEmpty() const {\n"
                                                    "    return _num_filled==0;\n"
                                                    "}\n"
                                                    "\n"
                                                    "size_t HashTable::getNumberOfBuckets() const {\n"
                                                    "    return _num_buckets;\n"
                                                    "}\n"
                                                    "\n"
                                                    "float HashTable::load_factor() const {\n"
                                                    "    return static_cast<float>(_num_filled) / static_cast<float>(_num_buckets);\n"
                                                    "}\n";
  }

 public:
  LRB2(bool inMemory = true) {
    m_name = "LRB2";
    createSchema();
    createApplication();
    m_fileName = "lrb-data-small-ht.txt";
    if (inMemory)
      loadInMemoryData();
  }
};
