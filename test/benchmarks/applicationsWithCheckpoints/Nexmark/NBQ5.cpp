#include <compression/Compressor.h>

#include "benchmarks/applications/Nexmark/Nexmark.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace NBQ5Compress {
struct alignas(16) input_tuple_t {
  long timestamp;
  long id;
  long itemName;
  long description;
  long initialBid;
  long reserve;
  long expires;
  long seller;
  long category;
  long padding_0;
  long padding_1;
  long padding_2;
  long padding_3;
  long padding_4;
  long padding_5;
  long padding_6;
};

std::vector<std::string, tbb::cache_aligned_allocator<std::string>> *metadata;

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  BaseDeltaCompressor<long, uint16_t> bcomp1(data[0].timestamp);
  BaseDeltaCompressor<long, uint16_t> bcomp2(data[0].id);
  struct t_1 {
    uint16_t timestamp : 6;
    uint16_t counter : 10;
  };
  struct t_2 {
    uint16_t id : 9;
    uint8_t counter : 7;
  };

  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }
  t_1 *buf1 = (t_1 *)(output);
  t_2 *buf2 = (t_2 *)(output + (int) (length*0.4));
  size_t n = (end - start) / sizeof(input_tuple_t);
  uint8_t count_1 = 1;
  uint8_t count_2 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    auto fVal_1 = bcomp1.compress(data[idx].timestamp);
    auto sVal_1 = fVal_1;
    if (idx < n - 1 && count_1 < 1024 && fVal_1 ==
        (sVal_1 = bcomp1.compress(data[idx + 1].timestamp))) {
      count_1++;
    } else {
      buf1[idxs[0]++] = {fVal_1, count_1};
      fVal_1 = sVal_1;
      count_1 = 1;
    }

    auto fVal_2 = bcomp2.compress(data[idx].id);
    auto sVal_2 = fVal_2;
    if (idx < n - 1 && count_2 < 512 && fVal_2 ==
        (sVal_2 = bcomp2.compress(data[idx + 1].id))) {
      count_2++;
    } else {
      buf2[idxs[1]++] = {fVal_2, count_2};
      fVal_2 = sVal_2;
      count_2 = 1;
    }
  }

  writePos += idxs[0] * sizeof(t_1);
  //(*metadata)[pid] = "c0 RLE BD " + std::to_string(data[0].timestamp) + " {uint16_t:6,uint16_t:10} " + std::to_string(writePos);
  std::memcpy((void *)(output + writePos), (void *)buf2,
              idxs[1] * sizeof(t_2));
  writePos += idxs[1] * sizeof(t_2);
  //(*metadata)[pid] = "c1 RLE BD " + std::to_string(data[0].id) + " {uint16_t:7,uint16_t:9} " + std::to_string(writePos);

  if (SystemConf::getInstance().LATENCY_ON) {
    auto value = data[0].timestamp;
    latency = (int) (value >> 32);
    (*metadata)[pid] += " " + std::to_string(latency) + " ";
  }
  //(*metadata)[pid] = "r0 " + std::to_string(idxs[0]) + " r1 " + std::to_string(idxs[1]) + " ";
  //if ((*metadata)[pid].size() > 128) {
  //  throw std::runtime_error("error: increase the metadata size");
  //}
  //std::memcpy((void *)(output - 128), (*metadata)[pid].data(), (*metadata)[pid].size());
  //(*metadata)[pid].clear();
}

struct tempV {
  int _1;
};
std::vector<tempV> tempVec[20];
bool isFirst[20] = {false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false};
void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2);
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  Simple8 simpleComp;
  //writePos = 0;
  // compress
  for (auto &i : idxs) {
    i = 0;
  }

  size_t n = (end - start) / sizeof(input_tuple_t);
  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);

  // simple 8
  auto buf2 = (uint64_t *)(output + (int) (length*0.5));
  int32_t inOffset = 0;
  int32_t outOffset = 0;
  idxs[1] = simpleComp.compress(
      inOffset, outOffset, n, &data, &input_tuple_t::id, 1, buf2);

  if (!isFirst[pid]) {
    tempVec[pid].resize(n);
    isFirst[pid] = true;
  }

  uint8_t count_2 = 1;
  uint16_t count_3 = 1;
  for (size_t idx = 0; idx < n; idx++) {
    if (idx > 1) {
      auto [deltaD, deltaLength] = gorillaComp.compress(
          data[idx].timestamp, data[idx - 1].timestamp,
          data[idx - 2].timestamp);
      if (count_1 + deltaLength > 64) {
        uint8_t split = (64 - count_1);
        if (deltaLength > 1) {
          buf1[idxs[0]] |= deltaD >> (deltaLength - split);
        }
        ++idxs[0];
        count_1 = deltaLength - split;
      } else {
        count_1 += deltaLength;
      }
      buf1[idxs[0]] |= deltaD << (64 - count_1);
    }
  }

  std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
              idxs[1] * sizeof(uint64_t));
  writePos += idxs[0] * sizeof(uint64_t) + idxs[1] * sizeof(uint64_t) +
      idxs[2] * sizeof(uint64_t);
}

void noCompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  struct res {
    long timestamp;
    long id;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx].id};
  }
  writePos = n * sizeof(res);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  // parse metadata
  std::string meta;
  if (copy) {
    std::memcpy(output, input, end);
    for (size_t i = 0; i < 128; i++) {
      meta += input[i];
    }
  } else {
    for (size_t i = 0; i < 128; i++) {
      meta += output[i];
    }
  }

  std::istringstream iss(meta);
  std::vector<std::string> words{std::istream_iterator<std::string>{iss},
                                 std::istream_iterator<std::string>{}};

  throw std::runtime_error("error: fix decompression..");
  auto base = std::stoi(words[3]);
  std::vector<size_t> idxs (3);
  idxs[0] = std::stoi(words[5]);
  idxs[1] = std::stoi(words[9]);
  if (SystemConf::getInstance().LATENCY_ON) {
    latency = std::stoi(words[10]);
  }


  //BaseDeltaCompressor<long, uint16_t> bcomp(base);
  struct t_1 {
    uint16_t timestamp : 6;
    uint16_t counter : 10;
  };
  struct t_2 {
    uint16_t id : 9;
    uint8_t counter : 7;
  };

  auto res = (input_tuple_t*) input;
  t_1 *col0 = (t_1 *)(output + 128);
  auto *col7 = (t_2 *)(output + 128 + idxs[0]);
  auto wPos = 0;
  auto dataSize = end / sizeof(input_tuple_t);
  auto col1Size = idxs[0] / sizeof(t_1);
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col0[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].timestamp = temp.timestamp + base;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }

  if (SystemConf::getInstance().LATENCY_ON) {
    res[0].timestamp = Utils::pack(latency, (int)res[0].timestamp);
  }

  // c0
  wPos = 0;
  for (int idx = 0; idx < col1Size; ++idx) {
    auto temp = col7[idx];
    for (int it = 0; it < temp.counter; ++it) {
      res[wPos++].id = temp.id;
      if (wPos * sizeof(input_tuple_t) > SystemConf::getInstance().BATCH_SIZE) {
        throw std::runtime_error("error: the write position exceeds the batch size");
      }
    }
  }


  writePos = wPos * sizeof(input_tuple_t);

  /*std::cout << "===========decompress===========" << std::endl;
  auto n = dataSize;
  for (int i = 0; i <n; i++) {
    std::cout << i << " " << res[i].timestamp << " " << res[i].category <<
    " " << res[i].cpu << std::endl;
  }
  std::cout << "======================" << std::endl;*/
}

void onlyCompressInputLossless(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    return;
  }

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char*)(output), &output_length);
  writePos += output_length;
}
};

class NBQ5 : public Nexmark {
 private:
  void createApplication() override {
    SystemConf::getInstance().SLOTS = 128;
    SystemConf::getInstance().PARTIAL_WINDOWS = 64;
    SystemConf::getInstance().HASH_TABLE_SIZE = 512;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

    // auto window = new WindowDefinition(RANGE_BASED, 3600, 60);
    auto window = new WindowDefinition(RANGE_BASED, 60, 1);

    // Configure aggregation
    std::vector<AggregationType> aggregationTypes(1);
    aggregationTypes[0] = AggregationTypes::fromString("cnt");

    std::vector<ColumnReference *> aggregationAttributes(1);
    aggregationAttributes[0] = new ColumnReference(1, BasicType::Float);

    std::vector<Expression *> groupByAttributes(1);
    groupByAttributes[0] = new ColumnReference(1, BasicType::Long);

    Aggregation *aggregation = new Aggregation(*window, aggregationTypes, aggregationAttributes, groupByAttributes);

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = window->isRangeBased();
#endif

    OperatorCode *cpuCode;
    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge);
    genCode->setInputSchema(getSchema());
    genCode->setAggregation(aggregation);
    //genCode->setCustomHashTable(customHashtable);
    genCode->setPostWindowOperation(postOperation, postCondition, (useParallelMerge) ? parallelMergeOperation : mergeOperation);
    genCode->setQueryId(0);
    genCode->setup();
    cpuCode = genCode;

    // Print operator
    std::cout << cpuCode->toSExpr() << std::endl;

    // Define an ft-operator
    auto queryOperator = new QueryOperator(*cpuCode, true);
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
                                         useParallelMerge,
                                         0, persistInput, nullptr, !SystemConf::getInstance().RECOVER);

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(NBQ5Compress::compressInput);
      queries[0]->getBuffer()->setDecompressionFP(NBQ5Compress::decompressInput);
      NBQ5Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      //NBQ5Compress::metadata = new std::vector<std::string, tbb::cache_aligned_allocator<std::string>>(SystemConf::getInstance().WORKER_THREADS, "");
      //m_application->getCheckpointCoordinator()->setCompressionFP(0, NBQ5Compress::compress);
    }
  }

  std::string parallelMergeOperation =
      "        int _max = INT_MIN;\n"
      "        for (int idx = 0; idx < mapSize; idx++) {\n"
      "          if (tempCompleteWindowsRes[idx].state == 1 && _max < tempCompleteWindowsRes[idx].value._1) /* Skip empty slot */\n"
      "            _max = tempCompleteWindowsRes[idx].value._1; \n"
      "        }\n";

  std::string mergeOperation =
      "            int _max = INT_MIN;\n"
      "            for (int idx = 0; idx < mapSize; idx++) {\n"
      "                if (openingWindowsRes[wid][idx].state != 1) /* Skip empty slot */\n"
      "                    continue;\n"
      "                isFound = map2.get_index(openingWindowsRes[wid][idx].key, posInB2);\n"
      "                if (posInB2 < 0) {\n"
      "                    printf(\"error: open-adress hash table is full \\n\");\n"
      "                    exit(1);\n"
      "                }\n"
      "                if (!isFound) {                        \n"
      "                    _max = (_max > openingWindowsRes[wid][idx].value._1) ? _max : openingWindowsRes[wid][idx].value._1;\n"
      "                } else { // merge values based on the number of aggregated values and their types!            \n"
      "                    int temp = openingWindowsRes[wid][idx].value._1+partialRes[wid2][posInB2].value._1;\n"
      "                    _max = (_max > temp) ? _max : temp;\n"
      "                }\n"
      "            }\n"
      "\n"
      "            /* Iterate over the remaining tuples in the second table. */\n"
      "            for (int idx = 0; idx < mapSize; idx++) {\n"
      "                if (partialRes[wid2][idx].state == 1 && _max < partialRes[wid2][idx].value._1) /* Skip empty slot */\n"
      "                  _max = partialRes[wid2][idx].value._1;\n"
      "            }\n";

  std::string postOperation = "\tint _max = INT_MIN;\n"
                              "\tfor (int i = 0; i < mapSize; i++) {\n"
                              "\t\tif (aggrStructures[pid].getBuckets()[i].state == 1 && _max < aggrStructures[pid].getBuckets()[i].value._1)\n"
                              "\t\t\t_max = aggrStructures[pid].getBuckets()[i].value._1;\n"
                              "\t}\n";

  std::string postCondition = "completeWindowsResults[completeWindowsPointer]._2 == _max";

  std::string customHashtable = "using KeyT = long;\n"
      "using ValueT = Value;\n"
      "\n"
      "struct MyHash{\n"
      "    std::size_t operator()(KeyT m) const {\n"
      "        std::hash<KeyT> hashVal;\n"
      "        return hashVal(m%1000);\n"
      "    }\n"
      "};\n"
      "struct HashMapEqualTo {\n"
      "    constexpr bool operator()(const KeyT& lhs, const KeyT& rhs) const {\n"
      "        return lhs == rhs;\n"
      "    }\n"
      "};\n"
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
      "    void setIntermValues (int pos, long timestamp);\n"
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
      "    _buckets  = (BucketT*)_mm_malloc(_num_buckets * sizeof(BucketT), 64);\n"
      "    _aggrs  = (AggrT*)_mm_malloc(_num_buckets * sizeof(AggrT), 64);\n"
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
      "    for (; i < _num_buckets; i++) {\n"
      "        tempState = _buckets[i].state;\n"
      "        if (tempState && _eq(_buckets[i].key, key)) {\n"
      "\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
      "            _buckets[i].counter++;\n"
      "            return;\n"
      "        }\n"
      "        if (!tempState && (_eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
      "            _buckets[i].state = 1;\n"
      "            _buckets[i].dirty = 1;\n"
      "            _buckets[i].timestamp = timestamp;\n"
      "            _buckets[i].key = key;\n"
      "            _buckets[i].value = value;\n"
      "            _buckets[i].counter = 1;\n"
      "            return;\n"
      "        }\n"
      "    }\n"
      "    for (i = 0; i < ind; i++) {\n"
      "        tempState = _buckets[i].state;\n"
      "        if (tempState && _eq(_buckets[i].key, key)) {\n"
      "\t\t\t\t_buckets[i].value._1 = _buckets[i].value._1+value._1;\n"
      "            _buckets[i].counter++;\n"
      "            return;\n"
      "        }\n"
      "        if (!tempState && (_eq(_buckets[i].key, key) || _buckets[i].dirty == 0)) { // first insert -- keep track of previous inserted value\n"
      "            _buckets[i].state = 1;\n"
      "            _buckets[i].dirty = 1;\n"
      "            _buckets[i].timestamp = timestamp;\n"
      "            _buckets[i].key = key;\n"
      "            _buckets[i].value = value;\n"
      "            _buckets[i].counter = 1;\n"
      "            return;\n"
      "        }\n"
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
      "void HashTable::setIntermValues (int pos, long timestamp) {\n"
      "    for (auto i = 0; i < _num_buckets; ++i) {\n"
      "        if (_aggrs[i].addedElements - _aggrs[i].removedElements > 0) {\n"
      "            auto res = _aggrs[i].queryIntermediate (pos);\n"
      "            _buckets[i].state = 1;\n"
      "            _buckets[i].timestamp = timestamp;\n"
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
      "    index = -1;\n"
      "    for (; i < _num_buckets; i++) {\n"
      "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
      "            index = i;\n"
      "            return true;\n"
      "        }\n"
      "        if (_buckets[i].state == 0 && index == -1) {\n"
      "            index = i;\n"
      "        }\n"
      "    }\n"
      "    for (i = 0; i < ind; i++) {\n"
      "        if (_buckets[i].state && _eq(_buckets[i].key, key)) {\n"
      "            index = i;\n"
      "            return true;\n"
      "        }\n"
      "        if (_buckets[i].state == 0 && index == -1) {\n"
      "            index = i;\n"
      "        }\n"
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

 public:
  explicit NBQ5(bool inMemory = true, bool startApp = true) {
    m_name = "NBQ5";
    createSchema();
    if (inMemory)
      loadInMemoryData();
    if (startApp)
      createApplication();
  }
};