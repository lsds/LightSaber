#include "benchmarks/applications/YahooBenchmark/YahooBenchmark.h"
#include "compression/Compressor.h"
#include "cql/expressions/ColumnReference.h"
#include "cql/expressions/IntConstant.h"
#include "cql/operators/Aggregation.h"
#include "cql/operators/AggregationType.h"
#include "cql/operators/codeGeneration/OperatorKernel.h"
#include "cql/predicates/ComparisonPredicate.h"
#include "snappy.h"
#include "utils/Query.h"
#include "utils/QueryOperator.h"
#include "utils/WindowDefinition.h"

namespace YSBCompress {
struct alignas(16) input_tuple_t {
  long timestamp;
  long _1;
  __uint128_t _2;
  __uint128_t _3;
  __uint128_t _4;
  long _5;
  long _6;
  __uint128_t _7;
  __uint128_t _8;
  __uint128_t _9;
};
struct hash {
  std::size_t operator()(const __uint128_t &key) const {
    std::hash<int> hasher;
    return hasher((int)key);
  }
};
struct Eq {
  constexpr bool operator()(const __uint128_t &k1,
                            const __uint128_t &k2) const {
    return k1 == k2;
  }
};
using Key = __uint128_t;
using Value = uint16_t;
std::vector<std::unique_ptr<DictionaryCompressor<Key, uint16_t, hash, Eq>>> *dcomp;

struct dBucket {
  __uint128_t key;
};

struct HMEqualTo {
  constexpr bool operator()(const Key& lhs, const Key& rhs) const {
    return lhs == rhs;
  }
};
struct UInt128Hash {
  UInt128Hash() = default;
  inline std::size_t operator()(__uint128_t data) const {
    const __uint128_t __mask = static_cast<std::size_t>(-1);
    const std::size_t __a = (std::size_t)(data & __mask);
    const std::size_t __b = (std::size_t)((data & (__mask << 64)) >> 64);
    auto hasher = std::hash<size_t>();
    return hasher(__a) + hasher(__b);
  }
};
using MyHash = UInt128Hash;


using std::numeric_limits;

template <typename T, typename U>
bool CanTypeFitValue(const U value) {
  const intmax_t botT = intmax_t(numeric_limits<T>::min() );
  const intmax_t botU = intmax_t(numeric_limits<U>::min() );
  const uintmax_t topT = uintmax_t(numeric_limits<T>::max() );
  const uintmax_t topU = uintmax_t(numeric_limits<U>::max() );
  auto b =  !( (botT > botU && value < static_cast<U> (botT)) || (topT < topU && value > static_cast<U> (topT)) );
  if (!b) {
    std::cout << "can't fit" << std::endl;
  }
  return b;
}
template <typename In, typename Out>
class BaseDeltaCompressor2 {
 private:
  In m_base;

 public:
  BaseDeltaCompressor2(In base) : m_base(base) {}
  inline Out compress(In &input) { return (Out) std::abs(m_base - input); }
  inline bool check(In &input) {
    auto res = input - m_base;
    auto b = !CanTypeFitValue<Out, In>(res);
    return b;
  }
  inline std::string getBase() {
    return std::to_string(m_base);
  }
};

#include <memory>
static const int numOfWorkers = 20;
static const int numOfCols = 2;
static std::unique_ptr<DictionaryCompressor<Key, uint16_t, MyHash, HMEqualTo>> dcomp2[numOfWorkers][numOfCols];
static std::string metadata[numOfWorkers][numOfCols];
static bool isFirst [numOfWorkers] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
inline void compressInput1(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency) {
  if (start == 0 && end == -1) {
    // write metadata
    return;
  }
  if (isFirst[pid]) {
    dcomp2[pid][1] = std::make_unique<DictionaryCompressor<Key, uint16_t, MyHash, HMEqualTo>>(1024);
    isFirst[pid] = false;
  }
  if (clear) {
    dcomp2[pid][1]->clear();
    clear = false;
  }

  if (start == end || end < start) {
    return;
  }

  // Input Buffer
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (2, 0);
  uint16_t count_0 = 1;
  BaseDeltaCompressor2<long, uint8_t> comp_0(data[0].timestamp);
  auto temp_0 = comp_0.compress(data[0].timestamp);
  struct t_0 {
    uint8_t _0 : 4;
    uint16_t counter : 10;
  };
  struct t_1 {
    uint16_t _1 : 10;
  };
  // output buffers
  int barriers[2];
  barriers[0] = (int)(length*0.000000);
  t_0 *buf0 = (t_0 *) (output + barriers[0]);
  barriers[1] = (int)(length*0.500000);
  t_1 *buf1 = (t_1 *) (output + barriers[1]);
  size_t n = (end - start) / sizeof(input_tuple_t);

  for (size_t idx = 0; idx < n; idx++) {
    if ( data[idx]._6 == 0 )
    {
      // apply compression
      if (comp_0.check(data[idx].timestamp)) {
        std::cout << "warning: falling back to the original compression scheme"<< std::endl;
        clear = true;
        return;
      }
      auto res_0 = comp_0.compress(data[idx].timestamp);
      // apply RLE
      if (temp_0 != res_0 || count_0 >= 1023.000000) {
        buf0[idxs[0]++] = {temp_0, count_0};
        count_0 = 0;
        temp_0 = res_0;
      } else {
        count_0++;
      }
      auto res_1 = dcomp2[pid][1]->compress(data[idx]._4);
      buf1[idxs[1]++] = {res_1};
    }
  }
  if (count_0 != 0) {
    buf0[idxs[0]++] = {temp_0, count_0};
  }
  // copy results and set output pointers
  writePos += idxs[0] * sizeof(t_0);
  if (writePos > barriers[1]) {throw std::runtime_error("error: larger barriers needed");}
  std::memcpy((void *)(output + writePos), (void *)buf0, idxs[0] * sizeof(t_0));
  writePos += idxs[1] * sizeof(t_1);
  if (writePos > length) {throw std::runtime_error("error: larger barriers needed");}
  //write metadata
  writePos = 0;
  metadata[pid][0] = "";
  metadata[pid][0] += "0 ""RLE ""BD "+comp_0.getBase()+" ""{long:2;uint16_t:10;} " + std::to_string(writePos) + " ";
  writePos += idxs[0] * sizeof(t_0);
  metadata[pid][0] += std::to_string(writePos) + " ";
  auto endPtr = idxs[0] * sizeof(t_0)+idxs[1] * sizeof(t_1);
  auto dcompSize = dcomp2[pid][1]->getTable().bucket_size() * dcomp2[pid][1]->getTable().max_size();
  metadata[pid][0] += "4 ""D "+std::to_string(endPtr)+" "+std::to_string(endPtr+dcompSize)+" ""{__uint128_t:10;} " + std::to_string(writePos) + " ";
  writePos += idxs[1] * sizeof(t_1);
  metadata[pid][0] += std::to_string(writePos) + " ";
  if (metadata[pid][0].size() > 128) { throw std::runtime_error("error: increase the size of metadata"); }
  std::memcpy((void *)(output - 128), (void *)metadata[pid][0].data(), metadata[pid][0].size());
}

void compressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    // static hashtable that never changes!
    /*auto buf = (dBucket*) (output + writePos);
    auto bucket = (Bucket<Key, Value> *)(*dcomp)[pid]->getTable().buckets();
    for (size_t idx = 0; idx < (*dcomp)[pid]->getTable().max_size(); ++idx) {
      if (bucket[idx].state) {
        buf[idx] = dBucket{bucket[idx].key};
      } else {
        buf[idx] = dBucket{(__uint128_t) -1};
      }
    }*/
    writePos += 0; //(*dcomp)[pid]->getTable().max_size() * sizeof(dBucket);
    return;
  }
  if (clear) {
    (*dcomp)[pid]->clear();
    clear = false;
  }

  BaseDeltaCompressor<long, uint16_t> bcomp(1000);
  struct res {
    uint16_t timestamp : 4;
    uint16_t user_id : 10;
  };

  auto data = (input_tuple_t *)input;
  res *buf = (res *)(output);
  size_t n = (end - start) / sizeof(input_tuple_t);
  writePos = 0;
  for (size_t idx = 0; idx < n; idx++) {
    if (data[idx]._6 == 0) {
      buf[writePos++] = {bcomp.compress(data[idx].timestamp),
                  (*dcomp)[pid]->compress(data[idx]._4)};
    }
  }
  writePos = writePos * sizeof(res);
}

void compressGenInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    // static hashtable that never changes!
    /*auto buf = (dBucket*) (output + writePos);
    auto bucket = (Bucket<Key, Value> *)(*dcomp)[pid]->getTable().buckets();
    for (size_t idx = 0; idx < (*dcomp)[pid]->getTable().max_size(); ++idx) {
      if (bucket[idx].state) {
        buf[idx] = dBucket{bucket[idx].key};
      } else {
        buf[idx] = dBucket{(__uint128_t) -1};
      }
    }*/
    writePos += 0; //(*dcomp)[pid]->getTable().max_size() * sizeof(dBucket);
    return;
  }
  if (clear) {
    //(*dcomp)[pid]->clear();
    clear = false;
  }

  auto data = (input_tuple_t *)input;
  int idxs[2] = {0, 0};
  GorillaTimestampCompressor<long, uint64_t> gorillaComp;
  size_t n = (end - start) / sizeof(input_tuple_t);

  // gorilla timestamp
  auto buf1 = (uint64_t *)output;
  uint8_t count_1 = 14;  // as the first delta is stored in 14 bits
  // store first timestamp in 64bits + first delta int 14 bits
  buf1[idxs[0]++] = data[0].timestamp;
  int64_t newDelta = data[1].timestamp - data[0].timestamp;
  buf1[idxs[0]] = newDelta << (64 - count_1);

  auto *buf2 = (__uint128_t *)(output + (int)(length* 0.5));
  writePos = 0;

  for (size_t idx = 0; idx < n; idx++) {
    if (data[idx]._6 == 0) {
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
      buf2[idxs[1]++] = data[idx]._4;//(*dcomp)[pid]->compress(data[idx]._4);
    }
  }
  size_t output_length;
  snappy::RawCompress((const char *)(buf2),
                      idxs[1] * sizeof(__int128_t), (char*)(output + idxs[0] * sizeof(uint64_t)),
                      &output_length);
  //std::memcpy((void *)(output + idxs[0] * sizeof(uint64_t)), (void *)buf2,
  //            idxs[1] * sizeof(uint16_t));
  writePos += idxs[0] * sizeof(uint64_t) + output_length;//idxs[1] * sizeof(uint16_t);
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
    __uint128_t userId;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  for (size_t idx = 0; idx < n; idx++) {
    out[idx] = {data[idx].timestamp, data[idx]._4};
  }
  writePos = n * sizeof(res);
}

void decompressInput(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &copy, long latency = -1) {
  throw std::runtime_error("error: the decompression function is not implemented");
}

void filterInput(char *input, int start, int end, char *output, int startOutput, int &writePos) {
  auto data = (input_tuple_t *)input;
  struct res {
    long timestamp;
    long _pad;
    __uint128_t userId;
  };
  auto out = (res*) output;
  size_t n = (end - start) / sizeof(input_tuple_t);
  size_t outIdx = startOutput;
  for (size_t idx = 0; idx < n; idx++) {
    //if (data[idx]._6 == 0) {
    //  out[outIdx++] = {data[idx].timestamp, 0, data[idx]._4};
    //}
    //std::memcpy(&out[idx], &data[idx], 64);
  }
  writePos = n * sizeof(res);
}

void onlyCompressInputLossless(int pid, char *input, int start, int end, char *output, int &writePos, int length, bool &clear, long latency = -1) {
  if (start == 0 && end == -1) {
    auto offset = 0;
    writePos += offset;
    return;
  }
  auto data = (input_tuple_t *)input;
  std::vector<size_t> idxs (1);
  for (auto &i : idxs) {
    i = 0;
  }

  size_t output_length;
  auto buf1 = (uint64_t *)input;
  snappy::RawCompress((const char *)(buf1), end, (char*)(output), &output_length);
  writePos += output_length;
}
};

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

  std::string getStaticHashTable(size_t adsNum) {
    auto tableSize = std::to_string(Utils::getPowerOfTwo(adsNum));
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
                                                                        "    int size = "+tableSize+";\n"
                                                                        "    int mask = size-1;\n"
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
    SystemConf::getInstance().HASH_TABLE_SIZE = Utils::getPowerOfTwo(SystemConf::getInstance().CAMPAIGNS_NUM);
    auto adsNum = Utils::getPowerOfTwo(SystemConf::getInstance().CAMPAIGNS_NUM * 10);
    //SystemConf::getInstance().CHECKPOINT_INTERVAL = 1000L;
    //SystemConf::getInstance().CHECKPOINT_ON =
    //    SystemConf::getInstance().CHECKPOINT_INTERVAL > 0;

    bool useParallelMerge = SystemConf::getInstance().PARALLEL_MERGE_ON;
    bool persistInput = SystemConf::getInstance().PERSIST_INPUT;

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
                                                    getStaticHashTable(adsNum),
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

#if defined(TCP_INPUT)
    bool replayTimestamps = false;
#elif defined(RDMA_INPUT)
    bool replayTimestamps = false;
#else
    bool replayTimestamps = window->isRangeBased();
#endif
    OperatorCode *cpuCode;
    // Set up code-generated operator
    OperatorKernel *genCode = new OperatorKernel(true, true, useParallelMerge, true);
    genCode->setInputSchema(getSchema());
    genCode->setSelection(selection);
    //genCode->setProjection(projection);
    genCode->setStaticHashJoin(staticJoin);
    genCode->setAggregation(aggregation);
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

#if defined(RDMA_INPUT)
    //queries[0]->getBuffer()->setFilterFP(YSBCompress::filterInput);
#endif

    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      queries[0]->getBuffer()->setCompressionFP(YSBCompress::compressInput);
      queries[0]->getBuffer()->setDecompressionFP(YSBCompress::decompressInput);
    }

    m_application = new QueryApplication(queries, SystemConf::getInstance().CHECKPOINT_ON, !SystemConf::getInstance().RECOVER);
    m_application->setup();
    if (SystemConf::getInstance().CHECKPOINT_COMPRESSION && (SystemConf::getInstance().CHECKPOINT_ON || persistInput)) {
      YSBCompress::dcomp = new std::vector<std::unique_ptr<
          DictionaryCompressor<YSBCompress::Key, uint16_t, YSBCompress::hash,
                               YSBCompress::Eq>>>();
      for (int w = 0; w < SystemConf::getInstance().WORKER_THREADS; ++w) {
        YSBCompress::dcomp->emplace_back(
            std::make_unique<DictionaryCompressor<YSBCompress::Key, uint16_t,
                                                  YSBCompress::hash, YSBCompress::Eq>>(adsNum));
      }
    }
    /*if (SystemConf::getInstance().CHECKPOINT_ON && SystemConf::getInstance().CHECKPOINT_COMPRESSION) {
      m_application->getCheckpointCoordinator()->setCompressionFP(0, YSBCompress::compress);
    }*/
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